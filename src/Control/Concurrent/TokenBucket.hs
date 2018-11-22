module Control.Concurrent.TokenBucket
    ( Bucket
    , Rate
    , BucketTake (..)

    , bucketNew
    , bucketTake

    , mkRate
    )
where

import           Data.Function (on)
import           Data.Int (Int64)
import           Prelude hiding (last)
import           System.Clock

type Duration = Int64

data Bucket = Bucket
    { added   :: Double
    , taken   :: Double
    , elapsed :: Duration
    , created :: TimeSpec
    }

instance Semigroup Bucket where
    a <> b = Bucket
        { added   = on max added a b
        , taken   = on max taken a b
        , elapsed = on max elapsed a b
        , created = on min created a b
        }

bucketNew :: IO Bucket
bucketNew = Bucket 0 0 0 <$> getTime Monotonic

data BucketTake =
      Fail { remaining :: Double }
    | Ok   { remaining :: Double }

bucketTake :: Bucket -> TimeSpec -> Rate -> Double -> (BucketTake, Bucket)
bucketTake b now rate n
    | n > have  = (Fail have, b)
    | otherwise =
        let
            b' = b { elapsed = elapsed b + toNanos elapsed'
                   , added   = added   b + added'
                   , taken   = taken   b + n
                   }
         in
            (Ok (added b' - taken b'), b')
  where
    capacity  = fromIntegral $ freq rate
    last      = case created b + fromNanos (elapsed b) of
                    x | now < x   -> now
                      | otherwise -> x
    tokens    = added b - taken b
    elapsed'  = now - last
    added'    = case rateTokens rate (toNanos elapsed') of
                    x | missing <- capacity - tokens
                      , x > missing -> missing
                      | otherwise   -> x
    have      = tokens + added'

    toNanos   = fromIntegral . toNanoSecs
    fromNanos = fromNanoSecs . fromIntegral

data Rate = Rate
    { freq :: Int
    , per  :: Duration
    }

mkRate :: Int -> Duration -> Rate
mkRate = Rate

rateTokens :: Rate -> Duration -> Double
rateTokens Rate { freq, per } d
    | zero      = 0
    | otherwise = fromIntegral d / fromIntegral interval
  where
    interval = per `div` fromIntegral freq
    zero     = or [freq == 0, per == 0, interval == 0]
