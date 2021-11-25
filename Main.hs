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
import qualified Data.Text.Internal.Search as T

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
import qualified Streamly.Internal.FileSystem.File as File

import Network.Socket
import Network.Socket.ByteString
import System.IO
import System.IO.Error
import System.IO.Unsafe
import System.Exit
import System.Process
import System.FilePath
import System.Environment

hostIP = "127.0.0.1"
ccPort = 8888
templateJson = "c:\\uxatto\\template.json"
dmPath = "c:\\uxatto"

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
  | CommandPause
  | CommandUnpause
  | CommandQuit
  deriving (Eq, Show)

type CommandID   = Int
type CommandText = Text
type StatusText  = Text

data CommandStatus = CS !(Maybe CommandID) !CommandText !StatusText
  deriving (Eq, Show)

data CommandChannel = CommandChannel Socket
  deriving (Eq, Show)

data Dm = Dm !ProcessHandle !CommandChannel

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
  WaitVmLogLine :: String -> BotRequest String
  Print :: String -> BotRequest ()
  Delay :: Int -> BotRequest ()
  SpawnVm :: String -> BotRequest ()
  ConnectVm :: BotRequest ()

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
               CommandPause -> [
                 "command" .= ("pause" :: Text)
                 ]
               CommandUnpause -> [
                 "command" .= ("unpause" :: Text)
                 ]
               CommandQuit -> [
                 "command" .= ("quit" :: Text)
                 ]

    idfield | Just id_ <- id   = ["id" .= (fromString (show id_) :: Text)]
            | otherwise        = []

waitVmState :: VmState -> Bot ()
waitVmState = prompt . WaitVmState

waitVmLogLine :: String -> Bot String
waitVmLogLine = prompt . WaitVmLogLine

delay :: Int -> Bot ()
delay = prompt . Delay

commandSend :: Command -> Bot CommandID
commandSend = prompt . SendCommand

commandWaitStatus :: CommandID -> Bot CommandStatus
commandWaitStatus = prompt . WaitCommandStatus

info :: String -> Bot ()
info = prompt . Print

spawnVm :: String -> Bot ()
spawnVm = prompt . SpawnVm

connectVm :: Bot ()
connectVm = prompt ConnectVm

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
resumeBot :: Int -> Bot ()
resumeBot ncycles = do
  connectVm >> waitVmState Running
  mapM_ cycle [1..ncycles]
  where
    cycle x = do
      info $ "CYCLE " ++ show x
      delay 20
      commandSendAndWait CommandSaveCuckoo >>= assertStatusOK
      commandSendAndWait CommandResume >>= assertStatusOK

-- suspend resume bot with aborted resumes
resumeAbortBot :: Int -> Bot ()
resumeAbortBot ncycles = do
  connectVm >> waitVmState Running
  delay 4000
  mapM_ cycle [1..ncycles]
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

restartBot :: String -> Int -> Bot ()
restartBot json ncycles = do
  mapM_ cycle [1..ncycles]
  where
    cycle x = do
      info $ "CYCLE " ++ show x ++ " --- " ++ json
      spawnVm json
      waitVmState Running
      --waitVmLogLine "Dropbear SSH server"
      delay 3000
      killVm
      delay 1000
    killVm = commandSendAndWait CommandQuit >>= assertStatusOK
    
pauseBot :: Int -> Bot ()
pauseBot ncycles = do
  connectVm >> waitVmState Running
  delay 60000
  mapM_ cycle [1..ncycles]
  where
    cycle x = do
      info $ "CYCLE " ++ show x
      delay 2000
      commandSendAndWait CommandPause >>= assertStatusOK
      delay 1000
      commandSendAndWait CommandUnpause >>= assertStatusOK
      
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

scanLog :: String -> String -> IO (Maybe String)
scanLog filename patt = do
  v <- S.head $
    File.toBytes filename
    & lineStream
    & S.map match
    & S.filter isJust & S.map fromJust
  case v of
    Just x -> return $ Just (T.unpack x)
    _ -> return Nothing
  where
    pattText = T.pack patt
    match :: Text -> Maybe Text
    match line = case T.indices pattText line of
      (index:_) -> (Just line)
      _ -> Nothing

handleBotPrompt :: MVar (Maybe CommandChannel) -> MVar CommandID -> BotRequest a -> IO a
handleBotPrompt cmdChannel cmdID req = handlePrompt req where

  setChannel channel = modifyMVar_ cmdChannel $ \_ -> return (Just channel)
  getChannel = readMVar cmdChannel

  nextCommandID = modifyMVar cmdID $ \id -> return (id+1, id)

  connectChannel = do
    Just (sock :: Socket) <- S.head $ serially $ S.unfold TCP.acceptOnPort ccPort
    setChannel (CommandChannel sock)
    putStrLn $ "!! connected!"

  handlePrompt (SendCommand cmd) = do
    Just (CommandChannel sock) <- getChannel
    id <- nextCommandID
    let cmdText    = T.decodeUtf8 cmdByteStr
        cmdByteStr = LB.toStrict (AE.encode $ commandToJSON (Just id) cmd)

    putStrLn $ " ---> " ++ (T.unpack cmdText)
    sendAll sock $ cmdByteStr `B.snoc` 10 -- eol
    return id

  handlePrompt (WaitCommandStatus cmdID) = do
    putStrLn ( "wait for status of .. " ++ show cmdID)
    Just ch <- getChannel
    Just v <- S.head $
      statusMessages ch
      & S.map match
      & S.filter isJust & S.map fromJust
    return v
    where
      match (CommandStatus cs@(CS (Just cmdID') _ _)) | cmdID == cmdID' = Just cs
      match _ = Nothing

  handlePrompt (Print s) = putStrLn s

  handlePrompt (WaitVmState s) = do
    putStrLn ("wait for vm state " ++ show s)
    Just ch <- getChannel
    S.head $
      statusMessages ch & S.filter match
    return ()
    where
      match (VmStateChanged s') | s == s' = True
      match _ = False

  handlePrompt (WaitVmLogLine s) = do
    putStrLn ("wait for vm log line " ++ show s)
    waitLine s
    where
      waitLine s = do
        r <- scanLog "uxendm.log" s
        case r of
          (Just l) -> return l
          _ -> threadDelay 2000 >> waitLine s
    
  handlePrompt (Delay ms) = threadDelay (1000 * ms)

  handlePrompt (SpawnVm conf) = do
    closeChannel =<< getChannel
    (_,_,_, ph) <- createProcess (uxendm conf)
    putStrLn $ "started dm from " ++ conf
    connectChannel
    where
      closeChannel (Just (CommandChannel sock)) = close sock
      closeChannel _ = return ()

  handlePrompt ConnectVm = connectChannel

  statusMessages :: CommandChannel -> SerialT IO VmStatusMessage
  statusMessages (CommandChannel sock) =
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

runBot :: Bot () -> IO ()
runBot bot = do
  cmdID <- newMVar 1
  cc <- newMVar Nothing
  runPromptM (handleBotPrompt cc cmdID) (unBot bot)

uxendm :: FilePath -> CreateProcess
uxendm confPath = proc (dmPath </> "uxendm.exe") ["-F", confPath, "-C", "tcp:" ++ hostIP ++ ":" ++ show ccPort]

runDm :: IO Dm
runDm = do
  (_,_,_, ph) <- createProcess (uxendm templateJson)
  putStrLn "started dm.."
  Just (sock :: Socket) <- S.head $ serially $ S.unfold TCP.acceptOnPort ccPort
  return (Dm ph (CommandChannel sock))
      
--runServer :: PortNumber -> (Bot ()) -> IO ()
--runServer port bot =
--  S.drain . S.take 1 $
--  S.unfold TCP.acceptOnPort port & S.mapM handleConnection
--  where
--    handleConnection s = (do
--      putStrLn "connected!"
--      runBot (CommandChannel s) bot
--      putStrLn "bot done") `finally` (close s)

botFromString :: String -> [Int -> Bot ()]
botFromString x = case x of
  "resume" -> [resumeBot]
  "resume-abort" -> [resumeAbortBot]
  "pause" -> [pauseBot]
  "restart" -> [restartBot "testatto.json"]
  "manyrestarts" -> map restartBot ["testatto.json", "testatto2.json", "testatto3.json", "testatto4.json"]
  _ -> []

main = withSocketsDo $ do
  args <- getArgs
  bots <- return $
        case args of
          (botStr : cyclesStr : _)
            | bots           <- botFromString botStr,
              [(ncycles,_)]  <- reads cyclesStr,
              not (null bots)
              -> map (\b -> b ncycles) bots
          _ -> error "bad args"
  putStrLn "starting..."
  threads <- mapM (\bot -> bot `seq` do
                           mvar <- newEmptyMVar
                           forkFinally (runBot bot) (\_ -> putMVar mvar ())
                           return mvar)
             bots
  mapM_ takeMVar threads
                           
  
