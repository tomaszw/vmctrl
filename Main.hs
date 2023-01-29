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
import System.Random

hostIP = "127.0.0.1"
ccPortBase = 8888
templateJson = "c:\\uxatto\\template.json"
dmPath = "."

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
  WaitVmLogLine :: String -> Int -> BotRequest (Maybe String)
  WaitEnter :: BotRequest ()
  Print :: String -> BotRequest ()
  Delay :: Int -> BotRequest ()
  RandomDelay :: (Int,Int) -> BotRequest ()
  CoinToss :: BotRequest Bool
  SpawnVm :: (String, Maybe String) -> BotRequest ()
  SnapshotVhd :: String -> String -> BotRequest ()
  ConnectVm :: BotRequest ()

commandToJSON :: Maybe CommandID -> Command -> AE.Value
commandToJSON id cmd =
  AE.object ( fields ++ idfield )
  where
    fields = case cmd of
               CommandSave ->  [
                 "command" .= ("save" :: Text),
                 "filename" .= ("savefile" :: Text)
                 ]
               CommandSaveCuckoo ->  [
                 "command"  .= ("save" :: Text),
                 "compress" .= ("cuckoo" :: Text),
                 "filename" .= ("savefile" :: Text)
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

waitVmLogLine :: String -> Int -> Bot (Maybe String)
waitVmLogLine str tmo = prompt (WaitVmLogLine str tmo)

waitEnter :: Bot ()
waitEnter = prompt WaitEnter

delay :: Int -> Bot ()
delay = prompt . Delay

randomDelay :: (Int,Int) -> Bot ()
randomDelay = prompt . RandomDelay

commandSend :: Command -> Bot CommandID
commandSend = prompt . SendCommand

commandWaitStatus :: CommandID -> Bot CommandStatus
commandWaitStatus = prompt . WaitCommandStatus

info :: String -> Bot ()
info = prompt . Print

spawnVm :: (String, Maybe String) -> Bot ()
spawnVm = prompt . SpawnVm

snapshotVhd :: String -> String -> Bot ()
snapshotVhd name par = prompt (SnapshotVhd name par)

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
  waitEnter
  mapM_ cycle [1..ncycles]
  where
    cycle x = do
      info $ "CYCLE " ++ show x
      delay 1500
      commandSendAndWait CommandSaveCuckoo >>= assertStatusOK
      commandSendAndWait CommandResume >>= assertStatusOK

resumeBot2 :: Int -> Bot ()
resumeBot2 ncycles = do
  connectVm >> waitVmState Running
  waitEnter
  mapM_ cycle [1..ncycles]
  where
    cycle x = do
      info $ "CYCLE " ++ show x
      delay 1500
      commandSendAndWait CommandSave >>= assertStatusOK
      commandSendAndWait CommandResume >>= assertStatusOK

-- suspend resume bot with aborted resumes
resumeAbortBot :: Int -> Bot ()
resumeAbortBot ncycles = do
  connectVm >> waitVmState Running
  waitEnter
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

saveAbortBot :: Int -> Bot ()
saveAbortBot ncycles = do
  connectVm >> waitVmState Running
  waitEnter
  mapM_ cycle [1..ncycles]
  where
    cycle x = do
      info $ "CYCLE " ++ show x
      -- suspend vm
      commandSend CommandSaveCuckoo
      commandSendAndWait CommandResume >>= assertStatusOK
      delay 1000

abortTortureBot :: Int -> Bot ()
abortTortureBot ncycles = do
  connectVm >> waitVmState Running
  waitEnter
  mapM_ cycle [1..ncycles]
  where
    cycle x = do
      info $ "CYCLE " ++ show x
      commandSend CommandSaveCuckoo
      randomDelay (0, 5000)
      commandSendAndWait CommandResume
      randomDelay (0, 5000)
      -- abort resume maybe, maybe not ?
      coin <- prompt CoinToss
      when coin $ do
        commandSendAndWait CommandResumeAbort
        randomDelay (0, 5000)

restartBot :: String -> Int -> Bot ()
restartBot json ncycles = do
  mapM_ cycle [1..ncycles]
  where
    cycle x = do
      info $ "CYCLE " ++ show x ++ " --- " ++ json
      spawnVm (json,Nothing)
      waitVmState Running
      --waitVmLogLine "Dropbear SSH server"
      delay 3000
      killVm
      delay 1000
    killVm = commandSendAndWait CommandQuit >>= assertStatusOK

resumeExitBot :: String -> Int -> Bot ()
resumeExitBot json ncycles = do
  mapM_ cycle [1..ncycles]
  where
    cycle x = do
      info $ "CYCLE " ++ show x
      spawnVm (json, Just "savefile")
      waitVmState Running
      delay 2000
      commandSendAndWait CommandSave >>= assertStatusOK
      commandSendAndWait CommandQuit >>= assertStatusOK
      delay 1000

eptfaultBot :: String -> Int -> Bot ()
eptfaultBot json ncycles = do
  cycle 1
  where
    waitTimeSecs = 55
    cycle x = do
      info $ "CYCLE " ++ show x ++ " --- " ++ json
      snapshotVhd "20h2-2.vhd" "20h2.vhd"
      spawnVm (json,Nothing)
      waitVmState Running
      line <- waitVmLogLine "Back-to-back" waitTimeSecs
      case line of
        Just l -> info $ "Back to back violations detected!! " ++ l
        _ -> do
          info $ "no violation, go on"
          killVm
          delay 4000
          when (x < ncycles) $ do
            cycle (x+1)
    killVm = commandSendAndWait CommandQuit >>= assertStatusOK

stallBot :: Int -> Bot ()
stallBot ncycles = do
  cycle 1
  where
    waitTimeSecs = 6
    cycle x = do
      info $ "CYCLE " ++ show x ++ " --- "
--      snapshotVhd "win11-stall-snap.vhd" "win11-2.vhd"
      spawnVm ("win11-stall.json",Just "savefile")
      waitVmState Running
      line <- waitVmLogLine "SYSTEM TIME" waitTimeSecs
      case line of
        Just l -> info $ "TIME STALL detected!! " ++ l
        _ -> do
          info $ "no stall, go on"
          commandSendAndWait CommandSave >>= assertStatusOK
          killVm
          delay 2400
          when (x < ncycles) $ do
            cycle (x+1)
    killVm = commandSendAndWait CommandQuit >>= assertStatusOK
  
pauseBot :: Int -> Bot ()
pauseBot ncycles = do
  connectVm >> waitVmState Running
  waitEnter
  mapM_ cycle [1..ncycles]
  where
    cycle x = do
      info $ "CYCLE " ++ show x
      delay 800
      commandSendAndWait CommandPause >>= assertStatusOK
      delay 800
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

handleBotPrompt :: MVar (Maybe CommandChannel) -> MVar CommandID -> PortNumber -> BotRequest a -> IO a
handleBotPrompt cmdChannel cmdID port req = handlePrompt req where

  setChannel channel = modifyMVar_ cmdChannel $ \_ -> return (Just channel)
  getChannel = readMVar cmdChannel

  nextCommandID = modifyMVar cmdID $ \id -> return (id+1, id)

  connectChannel = do
    Just (sock :: Socket) <- S.head $ serially $ S.unfold TCP.acceptOnPort port
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

  handlePrompt (WaitEnter) = do
    putStrLn "press ENTER"
    getLine
    return ()
    
  handlePrompt (WaitVmState s) = do
    putStrLn ("wait for vm state " ++ show s)
    Just ch <- getChannel
    S.head $
      statusMessages ch & S.filter match
    return ()
    where
      match (VmStateChanged s') | s == s' = True
      match _ = False

  handlePrompt (WaitVmLogLine s timeout) = do
    putStrLn ("wait for vm log line " ++ show s)
    waitLine s 0
    where
      waitLine s timepassed = do
        r <- scanLog "uxendm.log" s
        case r of
          (Just l) -> return (Just l)
          _ | (timeout > 0)
              && (timepassed >= timeout) -> do
                putStrLn "timeout"
                return Nothing
            | otherwise -> do
                threadDelay 1000000
                waitLine s (timepassed+1)
    
  handlePrompt (Delay ms) = threadDelay (1000 * ms)
  handlePrompt (RandomDelay (minms, maxms)) = do
    ms <- randomRIO (minms, maxms)
    threadDelay (1000 * ms)
  handlePrompt CoinToss = do
    v :: Int <- randomRIO (0, 1)
    return (v == 1)

  handlePrompt (SpawnVm (conf,templ)) = do
    closeChannel =<< getChannel
    (_,_,_, ph) <- createProcess (uxendm conf port templ)
    putStrLn $ "started dm from " ++ conf ++ " template: " ++ show templ
    connectChannel
    where
      closeChannel (Just (CommandChannel sock)) = close sock
      closeChannel _ = return ()

  handlePrompt ConnectVm = connectChannel
  handlePrompt (SnapshotVhd name par) = runSnapshotVhd name par
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

runBot :: PortNumber -> Bot () -> IO ()
runBot port bot = do
  cmdID <- newMVar 1
  cc <- newMVar Nothing
  runPromptM (handleBotPrompt cc cmdID port) (unBot bot)

uxendm :: FilePath -> PortNumber -> Maybe String -> CreateProcess
uxendm confPath port loadfile = proc (dmPath </> "uxendm.exe") $ ["-F", confPath, "-C", "tcp:" ++ hostIP ++ ":" ++ show port]
  ++ templ
  where
    templ | Just n <- loadfile = ["-l", n]
          | otherwise = []

runDm :: PortNumber -> IO Dm
runDm port = do
  (_,_,_, ph) <- createProcess (uxendm templateJson port Nothing)
  putStrLn "started dm.."
  Just (sock :: Socket) <- S.head $ serially $ S.unfold TCP.acceptOnPort port
  return (Dm ph (CommandChannel sock))

runSnapshotVhd :: String -> String -> IO ()
runSnapshotVhd snapshotName parentName = do
    retry 5
    where
      retry 0 = error $ "FAIL"
      retry n = do
        (_,_,_, ph) <- createProcess $ proc ("." </> "vhd-util.exe") ["snapshot", "-n", snapshotName, "-p", parentName]
        ex <- waitForProcess ph
        when (ex /= ExitSuccess) $ do
          putStrLn $ "bad vhdutil exit code " ++ show ex
          threadDelay (1000000)
          retry (n-1)
      
  
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
  "resume2" -> [resumeBot2]
  "resume-exit" -> [resumeExitBot "win11.json"]
  "resume-abort" -> [resumeAbortBot]
  "save-abort" -> [saveAbortBot]
  "stall" -> [stallBot]
  "abort-torture" -> [abortTortureBot]
  "pause" -> [pauseBot]
  "restart" -> [restartBot "testatto.json"]
  "eptfaults" -> [eptfaultBot "20h2.json"]
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
  threads <- mapM (\(bot,port) -> bot `seq` do
                           mvar <- newEmptyMVar
                           forkOS $ finally (runBot port bot) (putMVar mvar ())
                           return mvar)
             (zip bots [ccPortBase..])
  mapM_ takeMVar threads
                           
  
