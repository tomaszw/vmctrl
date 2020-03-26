module Main where

import qualified Data.Aeson as AE
import Data.Aeson ( (.=) )
import Data.String
import Data.Functor
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Encoding as T
import Data.Text (Text)
import Data.Text.Read
import Data.Word
import Data.Maybe
import Data.Function
import qualified Data.Map as M
import qualified Data.HashMap.Strict as HM
import Control.Monad.Prompt
import Control.Monad
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Exception

import Streamly
import qualified Streamly.Network.Socket as SN
import qualified Streamly.Prelude as S
import qualified Streamly.Data.Unicode.Stream as UnicodeS
import qualified Streamly.Network.Inet.TCP as TCP
import qualified Streamly.Data.Fold as FL

import Network.Socket
import Network.Socket.ByteString
import System.IO
import System.IO.Error
import System.IO.Unsafe
import System.Exit

data VmState =
    Running
  | Paused
  | Suspended
  | Shutdown
  deriving (Eq, Show)

data Command =
    CommandSave
  | CommandSaveCuckoo
  | CommandResume
  | CommandResumeAbort
  deriving (Eq, Show)

type CommandID   = Int
type CommandText = Text
type StatusText  = Text

data CommandStatus = CS !(Maybe CommandID) !CommandText !StatusText
  deriving (Eq, Show)

data VmStatusMessage =
     VmStateChanged !VmState
   | CommandStatus !CommandStatus
   deriving (Eq, Show)

type DictMessage = M.Map String AE.Value

-- Bot monad
newtype Bot a = Bot { unBot :: Prompt BotRequest a }
  deriving (Functor, Applicative, Monad, MonadPrompt BotRequest)

data BotRequest a where
  SendCommand :: Command -> BotRequest CommandID
  WaitCommandStatus :: CommandID -> BotRequest CommandStatus
  WaitVmState :: VmState -> BotRequest ()
  Print :: String -> BotRequest ()
  Delay :: Int -> BotRequest ()

commandToJSON :: Maybe CommandID -> Command -> AE.Value
commandToJSON id cmd =
  AE.object ( fields ++ idfield )
  where
    fields = case cmd of
               CommandSave ->  [
                 "command" .= ("save" :: Text)
                 ]
               CommandSaveCuckoo ->  [
                 "command"  .= ("save" :: Text),
                 "compress" .= ("cuckoo" :: Text),
                 "savefile" .= ("savefile" :: Text)
                 ]
               CommandResume -> [
                 "command" .= ("resume" :: Text)
                 ]
               CommandResumeAbort -> [
                 "command" .= ("resume-abort" :: Text)
                 ]

    idfield | Just id_ <- id   = ["id" .= (fromString (show id_) :: Text)]
            | otherwise        = []

waitVmState :: VmState -> Bot ()
waitVmState = prompt . WaitVmState

delay :: Int -> Bot ()
delay = prompt . Delay

commandSend :: Command -> Bot CommandID
commandSend = prompt . SendCommand

commandWaitStatus :: CommandID -> Bot CommandStatus
commandWaitStatus = prompt . WaitCommandStatus

info :: String -> Bot ()
info = prompt . Print

commandSendAndWait :: Command -> Bot CommandStatus
commandSendAndWait = commandSend >=> commandWaitStatus

assertStatusOK :: CommandStatus -> Bot ()
assertStatusOK s
  | isCommandStatusOK s = return ()
  | otherwise = error $ "command FAILED!"
  where
    isCommandStatusOK :: CommandStatus -> Bool
    isCommandStatusOK (CS _ _ "ok") = True
    isCommandStatusOK _ = False

-- simplest suspend resume bot
resumeBot :: Bot ()
resumeBot = do
  waitVmState Running
  mapM_ cycle [1..1000]
  where
    cycle x = do
      info $ "CYCLE " ++ show x
      commandSendAndWait CommandSaveCuckoo >>= assertStatusOK
      commandSendAndWait CommandResume >>= assertStatusOK

-- suspend resume bot with aborted resumes
resumeAbortBot :: Bot ()
resumeAbortBot = do
  waitVmState Running
  delay 4000
  mapM_ cycle [1..10]
  where
    cycle x = do
      info $ "CYCLE " ++ show x
      -- suspend vm
      commandSendAndWait CommandSaveCuckoo >>= assertStatusOK
      -- send resume
      resumeID <- commandSend CommandResume
      -- and immediately try to abort it
      commandSend CommandResumeAbort
      -- success or fail initial resume, dont care
      commandWaitStatus resumeID
      -- next resume must succeed
      commandSendAndWait CommandResume >>= assertStatusOK
      
parseVmState :: Text -> Maybe VmState
parseVmState "running"   = Just Running
parseVmState "paused"    = Just Paused
parseVmState "suspended" = Just Suspended
parseVmState "shutdown"  = Just Shutdown
parseVmState _           = Nothing

statusMessageStream :: (IsStream t, Monad m) => t m DictMessage -> t m VmStatusMessage
statusMessageStream =
    S.map fromJust . S.filter isJust
  . S.map parseStatusMessage
  where
    parseStatusMessage :: DictMessage -> Maybe VmStatusMessage
    parseStatusMessage m
      | Just runstateStr <- valueOf "vm-runstate"
      = VmStateChanged <$> parseVmState runstateStr

      | Just status <- valueOf "status"
      , Just cmd    <- valueOf "command"
      = Just (CommandStatus (CS id cmd status))

      | otherwise = Nothing
      where
        valueOf :: String -> Maybe Text
        valueOf k | Just (AE.String s) <- k `M.lookup` m = Just s
                  | otherwise = Nothing

        id :: Maybe CommandID
        id | Just (AE.String idText) <- "id" `M.lookup` m =
               case decimal idText of
                 Right (x, _) -> Just x
                 _            -> Nothing
           | otherwise = Nothing
          
    
    
dictMessageStream :: (IsStream t, Monad m) => t m Text -> t m DictMessage
dictMessageStream s =
  -- text to bytestring
    S.map (LB.fromStrict . T.encodeUtf8) s
  -- bytestring to maybe json
  & S.map jsonToDictMessage
  -- remove maybes
  & S.filter isJust & S.map fromJust
  where
    jsonToDictMessage :: LB.ByteString -> Maybe DictMessage
    jsonToDictMessage json = AE.decode json


lineStream :: (IsStream t, Monad m) => t m Word8 -> t m Text
lineStream s =
    UnicodeS.decodeUtf8 s
  & S.splitWithSuffix eol FL.toList
  & S.map T.pack
  & S.map (T.dropWhileEnd crlf)
  where
    eol  x = x == '\n'
    crlf x = x == '\n' || x == '\r'

handleBotPrompt :: Socket -> MVar CommandID -> BotRequest a -> IO a
handleBotPrompt sock cmdID req = handlePrompt req where

  nextCommandID = modifyMVar cmdID $ \id -> return (id+1, id)

  handlePrompt (SendCommand cmd) = do
    id <- nextCommandID
    let cmdText    = T.decodeUtf8 cmdByteStr
        cmdByteStr = LB.toStrict (AE.encode $ commandToJSON (Just id) cmd)

    putStrLn $ " ---> " ++ (T.unpack cmdText)
    sendAll sock $ cmdByteStr `B.snoc` 10 -- eol
    return id

  handlePrompt (WaitCommandStatus cmdID) = do
    putStrLn ( "wait for status of .. " ++ show cmdID)
    Just v <- S.head $
      statusMessages
      & S.map match
      & S.filter isJust & S.map fromJust
    return v
    where
      match (CommandStatus cs@(CS (Just cmdID') _ _)) | cmdID == cmdID' = Just cs
      match _ = Nothing

  handlePrompt (Print s) = putStrLn s

  handlePrompt (WaitVmState s) = do
    putStrLn ("wait for vm state " ++ show s)
    S.head $
      statusMessages & S.filter match
    return ()
    where
      match (VmStateChanged s') | s == s' = True
      match _ = False

  handlePrompt (Delay ms) = threadDelay (1000 * ms)

  statusMessages :: SerialT IO VmStatusMessage
  statusMessages =
        S.unfold SN.read sock
      & lineStream
      & S.mapM dump
      & dictMessageStream
      & statusMessageStream
    where
      trim s n | T.length s <= n = s
               | otherwise       = T.take n s `T.append` "..."
      dump l = do
        putStrLn $ " <--- " ++ (T.unpack $ trim l 100)
        return l

runBot :: Socket -> Bot () -> IO ()
runBot s bot = do
  cmdID <- newMVar 1
  runPromptM (handleBotPrompt s cmdID) (unBot bot)
   
runServer :: PortNumber -> IO ()
runServer port =
  S.drain . S.take 1 $
  S.unfold TCP.acceptOnPort port & S.mapM handleConnection
  where
    handleConnection s = (do
      putStrLn "connected!"
      runBot s resumeBot
      putStrLn "bot done") `finally` (close s)

main = withSocketsDo $ do
  runServer 8888
