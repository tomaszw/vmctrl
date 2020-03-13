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
import qualified Data.Map as M
import qualified Data.HashMap.Strict as HM
import Control.Monad.Prompt
import Control.Monad
import Control.Concurrent
import Control.Concurrent.Chan

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

-- suspend resume bot with aborted resumes
resumeAbortBot :: Bot ()
resumeAbortBot = do
  waitVmState Running
  delay 4000
  mapM_ cycle [1..1000]
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
      
-- simplest suspend resume bot
resumeBot :: Bot ()
resumeBot = do
  waitVmState Running
  mapM_ cycle [1..5]
  where
    cycle x = do
      info $ "CYCLE " ++ show x
      commandSendAndWait CommandSaveCuckoo >>= assertStatusOK
      commandSendAndWait CommandResume >>= assertStatusOK

parseVmState :: Text -> Maybe VmState
parseVmState "running"   = Just Running
parseVmState "paused"    = Just Paused
parseVmState "suspended" = Just Suspended
parseVmState "shutdown"  = Just Shutdown
parseVmState _           = Nothing

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
          
    
jsonToDictMessage :: LB.ByteString -> Maybe DictMessage
jsonToDictMessage json = AE.decode json

dictMessagesChan :: Handle -> IO (Chan DictMessage)
dictMessagesChan h = do
  ch <- newChan
  forkIO $ (forever $ do
    l <- B.hGetLine h
    putStrLn $ " <--- " ++ T.unpack (trim (T.decodeUtf8 l) 100)
    case jsonToDictMessage (LB.fromStrict l) of
      Just m -> writeChan ch m
      _      -> return ()) `catchIOError` onError
  return ch
  where
    onError err | isEOFError err = putStrLn "EOF!"
                | otherwise = putStrLn $ "channel error " ++ show err

    trim s n | T.length s <= n = s
             | otherwise       = T.take n s `T.append` "..."

nextStatusMessage :: Chan DictMessage -> IO VmStatusMessage
nextStatusMessage ch = do
  m <- readChan ch
  from $ parseStatusMessage m
  where
    from Nothing  = nextStatusMessage ch
    from (Just m) = return m

handleBotPrompt :: Handle -> MVar CommandID -> Chan DictMessage -> BotRequest a -> IO a
handleBotPrompt sockH cmdID ch req = handlePrompt req where

  nextCommandID = modifyMVar cmdID $ \id -> return (id+1, id)

  handlePrompt (SendCommand cmd) = do
    id <- nextCommandID
    let cmdText = T.decodeUtf8 $ LB.toStrict (AE.encode $ commandToJSON (Just id) cmd)
    putStrLn $ " ---> " ++ (T.unpack cmdText)
    T.hPutStrLn sockH cmdText
    return id

  handlePrompt (WaitCommandStatus cmdID) = do
    putStrLn ( "wait for status of .. " ++ show cmdID)
    loop
    where
      loop = nextStatusMessage ch >>= handle
        where
          handle (CommandStatus cs@(CS (Just cmdID') _ _)) | cmdID == cmdID' = return cs
          handle _ = loop

  handlePrompt (Print s) = putStrLn s

  handlePrompt (WaitVmState s) = do
    putStrLn ("wait for vm state " ++ show s)
    loop
    where
      loop = nextStatusMessage ch >>= handle
        where
          handle (VmStateChanged s') | s == s' = putStrLn "Got state!"
          handle _ = loop

  handlePrompt (Delay ms) = threadDelay (1000 * ms)

runBot :: Handle -> Bot () -> IO ()
runBot h bot = do
  messagesCh <- dictMessagesChan h
  cmdID <- newMVar 1
  runPromptM (handleBotPrompt h cmdID messagesCh) (unBot bot)
   
runServer :: PortNumber -> IO ()
runServer port = do
  sock <- socket AF_INET Stream defaultProtocol
  bind sock ( SockAddrInet port 0 )
  listen sock 1
  handleConnections sock
  where
    handleConnections sock = do
      (conn, peer) <- accept sock
      putStrLn "connected!"
      h <- socketToHandle conn ReadWriteMode
      --hSetBuffering h LineBuffering
      runBot h resumeBot
      hFlush h
      putStrLn "bot done"

main = withSocketsDo $ do
  runServer 8888
  exitWith ExitSuccess
