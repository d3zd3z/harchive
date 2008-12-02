----------------------------------------------------------------------
-- The MVarMonad.
----------------------------------------------------------------------

module MBox (
   MBox, AtomicOp,
   atomicLift,
   runMBox,

   -- From the state.
   get, gets, put, liftIO
) where

import Control.Concurrent
import Control.Monad.Reader
import Control.Monad.State.Strict

-- MVar monads are a pair of monads, an outer ReaderT containing an
-- MVar holding an internal state in an MVar.  There is an innter
-- StateT monad which carries the current state of the MVar for a
-- sequence of atomic operations.

-- The ReaderT holding internal state 'i' and resulting in 'a'.
type MBox i a = ReaderT (MVar i) IO a

-- A sequence of atomic operations, that can possibly modify the
-- state.  This is run within 'atomicLift' which atomically lifts the
-- operation into the surrounting MBox.
type AtomicOp i a = StateT i IO a

atomicLift :: AtomicOp i a -> MBox i a
-- Lift the sequence of atomic operations described by the AtomicOp
-- operation into the surrounding MBox such that they will be invoked
-- with the state set to the contents of the MBox.
atomicLift action = do
   box <- ask
   liftIO $ modifyMVar box $ \s -> do
      (x, s') <- runStateT action s
      return (s', x)

runMBox :: MBox i a -> i -> IO a
-- run the MBox, placing the initial state 'i' into the MVar.
runMBox action i = do
   box <- newMVar i
   runReaderT action box

-- $TODO
-- Since the MBox is a ReaderT, it is possible to get the mbox
-- directly, and use runReaderT lifted into IO to make a duplicate
-- MBox.  This is actually probably going to be common, since one big
-- use of these is for concurrent programming.  Need to come up with a
-- clean API to do this.
