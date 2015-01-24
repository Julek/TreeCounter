{-# LANGUAGE TupleSections #-}

{-|
Module      : TreeCounter
Description : Wait-free Tree Counter.
License     : BSD3
Maintainer  : Julian Sutherland (julian.sutherland10@imperial.ac.uk)

A wait-free tree counter. Creates a binary tree of counters, with each leaf associated with a thread. Leaves can be split, creating a new leaf for the current thread and another that can be used by another thread. Each thread will act on different leaves, meaning the actions are wait-free. A read is performed on the counter by recursively traversing it and summing the value of the counters in the nodes and leaves of the tree.
-}

module Data.NonBlocking.WaitFree.TreeCounter(TreeCounter(), TreeCounterIO, TreeCounterSTM, newTreeCounter, splitTreeCounter, incTreeCounter, readTreeCounter) where

import Control.Concurrent.STM (STM())
import Control.Concurrent.STM.TVar (TVar())
import Control.Monad (join)
import Control.Monad.Ref (MonadAtomicRef, newRef, readRef, writeRef, atomicModifyRef)
import Data.IORef(IORef())

-- |TreeCounter inside the IO Monad.
type TreeCounterIO = TreeCounter IORef
-- |TreeCounter inside the STM Monad.
type TreeCounterSTM = TreeCounter TVar

-- |A wait-free concurrent Tree Counter, a binary tree of counters, with each leaf associated with a thread. Leaves can be split, creating a new leaf for the current thread and another that can be used by another thread. Increments are wait-free as long as each thread performs them on different instance of TreeCounter split from an initial instance using 'splitTreeCounter', prone to ABA problem otherwise.
data TreeCounter r = TreeCounter (r (r (CounterTree r), r (CounterTree r)))
data CounterTree r = Node Integer (r (CounterTree r)) (r (CounterTree r)) | Leaf (r Integer)

-- |Creates a new instance of the 'TreeCounter' data type, instanciated to the value of the input, with type in the 'Integral' class. 
{-# SPECIALIZE newTreeCounter :: (Integral a) => a -> IO TreeCounterIO #-}
{-# SPECIALIZE newTreeCounter :: (Integral a) => a -> STM TreeCounterSTM #-}
newTreeCounter :: (MonadAtomicRef r m, Integral a) => a -> m (TreeCounter r)
newTreeCounter n = newRef (toInteger n) >>= newRef . Leaf >>=  newRef . join (,) >>= return . TreeCounter

-- |Splits a 'TreeCounter' instance, updating it to a new leaf and creating a new one, allowing another thread to increment the counter in a wait-free manner.
{-# SPECIALIZE splitTreeCounter :: TreeCounterIO -> IO TreeCounterIO #-}
{-# SPECIALIZE splitTreeCounter :: TreeCounterSTM -> STM TreeCounterSTM #-}
splitTreeCounter :: (MonadAtomicRef r m) => TreeCounter r -> m (TreeCounter r)
splitTreeCounter (TreeCounter tupleRef) = do
  (leafRef, rootRef) <- readRef tupleRef
  (Leaf lCountRef)   <- readRef leafRef
  lCount   <- readRef lCountRef
  leftRef  <- newRef 0 >>= newRef . Leaf
  rightRef <- newRef 0 >>= newRef . Leaf
  writeRef leafRef (Node lCount leftRef rightRef)
  writeRef tupleRef (leftRef, rootRef)
  newRef (rightRef, rootRef) >>= return . TreeCounter

-- |Increments the 'TreeCounter' in an atomic manner as long as this thread is the only thread incrementing the counter from this instance 'TreeCounter'
{-# SPECIALIZE incTreeCounter :: TreeCounterIO -> IO () #-}
{-# SPECIALIZE incTreeCounter :: TreeCounterSTM -> STM () #-}
incTreeCounter :: (MonadAtomicRef r m) => TreeCounter r -> m ()
incTreeCounter (TreeCounter tupleRef) = do
  (leafRef, _) <- readRef tupleRef
  (Leaf lCountRef) <- readRef leafRef
  atomicModifyRef lCountRef ((,()) . (+1))

-- |Reads the total value of the binary tree of counters associated with this instance of 'TreeCounter'.
{-# SPECIALIZE readTreeCounter :: (Num a) => TreeCounterIO -> IO a #-}
{-# SPECIALIZE readTreeCounter :: (Num a) => TreeCounterSTM -> STM a #-}
readTreeCounter :: (MonadAtomicRef r m, Num a) => TreeCounter r -> m a
readTreeCounter (TreeCounter tupleRef) = readRef tupleRef >>= readRef . snd >>= sumTree >>= return . fromInteger

{-# SPECIALIZE sumTree :: CounterTree IORef -> IO Integer #-}
{-# SPECIALIZE sumTree :: CounterTree TVar -> STM Integer #-}
sumTree :: (MonadAtomicRef r m) => CounterTree r -> m Integer
sumTree (Leaf lCountRef) = readRef lCountRef
sumTree (Node nCount leftRef rightRef) = do
  lCount <- readRef leftRef >>= sumTree
  rCount <- readRef rightRef >>= sumTree
  return (nCount + lCount + rCount)
  
