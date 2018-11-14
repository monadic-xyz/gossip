module Network.Gossip.Test.Helpers
    ( TaskQueue
    , newTaskQueue
    , scheduleTask
    , drainTaskQueue
    )
where

import           Control.Concurrent.Async (Async, async, wait)
import           Control.Concurrent.STM
                 ( TQueue
                 , atomically
                 , flushTQueue
                 , newTQueueIO
                 , writeTQueue
                 )
import           Control.Exception.Safe (throwString)
import           Data.Foldable (traverse_)
import           System.Timeout (timeout)

newtype TaskQueue = TaskQueue (TQueue (Async ()))

newTaskQueue :: IO TaskQueue
newTaskQueue = TaskQueue <$> newTQueueIO

scheduleTask :: TaskQueue -> IO () -> IO ()
scheduleTask (TaskQueue q) task = do
    task' <-
        async $
            timeout 1000000 task >>= \case
                Nothing -> throwString "Task timeout!"
                Just () -> pure ()
    atomically $ writeTQueue q task'

drainTaskQueue :: TaskQueue -> IO ()
drainTaskQueue (TaskQueue q) = go
  where
    go = do
        tasks <- atomically $ flushTQueue q
        case tasks of
            [] -> pure ()
            xs -> traverse_ wait xs >> go
