{-# language AllowAmbiguousTypes #-}
{-# language UndecidableInstances #-}
module VPF.Frames.VinylExts
  ( RSingleton(..)
  , RMonoid(..)

  -- (strictly) monotone index sequences
  , IncSeq
  , DecSeq
  , Monotone
  , ReplaceSubseq

  -- ss is subsequence of rs if RImage ss rs is strictly monotone
  , RecSubseq(..)
  , RSubseq

  -- special case: if RSubseq ss '[] rs rs', we can split rs into ss and rs'
  , RecQuotient
  , RQuotient
  ) where

import GHC.Exts (Any)

import Control.Lens (Iso, Iso', Lens)
import qualified Control.Lens as L

import qualified Data.Array as A
import Data.Bifunctor (first, second)
import qualified Data.Type.Equality as E

import Data.Vinyl (Rec(..), (<+>))
import Data.Vinyl.ARec (ARec(..))

import Data.Vinyl.Functor (Compose(..))
import Data.Vinyl.Lens (rcast)
import Data.Vinyl.TypeLevel

import Unsafe.Coerce (unsafeCoerce)


-- Class for any record that can be constructed from a single element

class RSingleton rec where
    rsingleton :: Iso (rec f '[a]) (rec f '[b]) (f a) (f b)

instance RSingleton Rec where
    rsingleton = L.iso (\(fa :& RNil) -> fa) (:& RNil)
    {-# inline rsingleton #-}

instance RSingleton ARec where
    rsingleton = L.iso (\(ARec afas) -> unsafeCoerce (afas A.! 0))
                       (\fa -> ARec (A.listArray (0, 0) [unsafeCoerce fa]))
    {-# inline rsingleton #-}


-- Generic monoid-like structure for any rec (RNil/<+>)

class RMonoid rec where
    rempty :: rec f '[]
    rappend :: rec f as -> rec f bs -> rec f (as ++ bs)

    rcons :: RSingleton rec => f a -> rec f as -> rec f (a ': as)
    rcons = rappend . L.review rsingleton
    {-# inline rcons #-}

instance RMonoid Rec where
    rempty = RNil
    {-# inline rempty #-}

    rappend = (<+>)
    {-# inline rappend #-}

    rcons = (:&)
    {-# inline rcons #-}


instance RMonoid ARec where
    rempty = ARec (A.array (0,-1) [])
    {-# inline rempty #-}

    rappend (ARec as) (ARec bs) =
      let
        (0, u1) = A.bounds as
        (0, u2) = A.bounds bs
        u3 = u1+1 + u2+1 - 1
      in
        ARec $ A.listArray (0, u3) (A.elems as ++ A.elems bs)
    {-# inline rappend #-}

    rcons fa (ARec afas) =
        let
          (0, u) = A.bounds afas
        in
          ARec (A.listArray (0, u+1) (unsafeCoerce fa : A.elems afas))
    {-# inline rcons #-}



-- (strictly) monotone Nat lists

type family IncSeq is = dec_is | dec_is -> is where
  IncSeq '[] = '[]
  IncSeq (i ': is) = S i ': IncSeq is

type DecSeq is dec_is = IncSeq dec_is ~ is


class Monotone (is :: [Nat])

instance Monotone '[]
instance (DecSeq is' dec_is', Monotone dec_is') => Monotone (Z ': is')
instance (DecSeq is' dec_is', Monotone (dec_i ': dec_is')) => Monotone (S dec_i ': is')


class (is ~ RImage ss rs, Monotone is)
    => ReplaceSubseq (ss :: [k]) (ss' :: [k]) (rs :: [k]) (rs' :: [k]) (is :: [Nat])
    | is rs -> ss
    , is ss rs' -> rs
    , is ss' rs -> rs'


-- trivial case

instance (rs ~ rs')
    => ReplaceSubseq '[] '[] rs rs' '[]

-- insert ss' at head

instance
    ( ReplaceSubseq '[] ss' rs rs' '[]
    , s' ~ r'
    )
    => ReplaceSubseq '[] (s' ': ss') rs (r' ': rs') '[]

-- delete ss

instance
    ( ReplaceSubseq ss '[] rs rs' dec_is'
    , DecSeq is' dec_is'
    , RImage ss (r ': rs) ~ is'
    , s ~ r
    )
    => ReplaceSubseq (s ': ss) '[] (r ': rs) rs' (Z ': is')


-- replace non-empty ss with non-empty ss'

instance
    ( ReplaceSubseq ss ss' rs rs' dec_is'
    , DecSeq is' dec_is'
    , RImage ss (r ': rs) ~ is'
    , s ~ r
    , s' ~ r'
    )
    => ReplaceSubseq (s ': ss) (s' ': ss') (r ': rs) (r' ': rs') (Z ': is') where

instance
    ( ReplaceSubseq ss ss' rs rs' (i ': dec_is')
    , DecSeq is' dec_is'
    , RImage ss (r ': rs) ~ IncSeq (RImage ss rs)
    , r ~ r'
    )
    => ReplaceSubseq ss ss' (r ': rs) (r' ': rs') (S i ': is') where


-- field subsequences (different from subsets)

class ReplaceSubseq ss ss' rs rs' is => RecSubseq rec ss ss' rs rs' is where
  rsubseqC :: Lens (rec f rs) (rec f rs') (rec f ss) (rec f ss')

  -- Isomorphism when ss' ~ '[]: splitting rs into ss and rs'
  --  view rsubseqC :: rs -> ss
  --  set rsubseqC RNil :: rs -> rs'
  --  view (rsubseqSplitC Refl) = view rsubseqC &&& set rsubseqC RNil
  --  rs :~: ss ++ rs'
  rsubseqSplitC :: ss' E.:~: '[] -> Iso' (rec f rs) (rec f ss, rec f rs')


-- RecSubseq implementation helpers

type family DiscriminateEmpty xs a b where
  DiscriminateEmpty '[]       a b = a
  DiscriminateEmpty (x ': xs) a b = b


impossibleList :: (x ': xs) E.:~: '[] -> a
impossibleList eq = E.castWith (transport eq) ()
  where
    transport :: ys E.:~: zs
              -> DiscriminateEmpty ys a b E.:~: DiscriminateEmpty zs a b
    transport E.Refl = E.Refl


keep :: Functor g => (s -> (a, s')) -> g s -> Compose g ((,) a) s'
keep split = Compose . fmap split


restore :: Functor g => (a -> s' -> s) -> Compose g ((,) a) s' -> g s
restore join = fmap (uncurry join) . getCompose


-- insert ss' at head

instance
    ( ReplaceSubseq '[] ss' rs rs' '[]
    , rs' ~ (ss' ++ rs)
    )
    => RecSubseq Rec '[] ss' rs rs' '[] where

    rsubseqC = L.lens (const RNil) (\rs ss' -> ss' <+> rs)
    {-# inline rsubseqC #-}

    rsubseqSplitC E.Refl = L.iso (\rs -> (RNil, rs)) (\(RNil, rs) -> rs)
    {-# inline rsubseqSplitC #-}


-- delete ss

instance
    ( RecSubseq Rec ss '[] rs rs' dec_is'
    , DecSeq is' dec_is'
    , RImage ss (r ': rs) ~ is'
    , s ~ r
    )
    => RecSubseq Rec (s ': ss) '[] (r ': rs) rs' (Z ': is') where
  rsubseqC f (r :& rs) = rsubseqC (f . (r :&)) rs
  {-# inline rsubseqC #-}

  rsubseqSplitC eq =
      L.withIso (rsubseqSplitC @Rec @ss eq) $ \split join ->
          L.iso (\(s :& rs)      -> first (s :&) (split rs))
                (\(s :& ss, rs') -> s :& join (ss, rs'))
  {-# inline rsubseqSplitC #-}


-- replace nonempty ss with nonempty ss'

instance
    ( RecSubseq Rec ss ss' rs rs' dec_is'
    , DecSeq is' dec_is'
    , RImage ss (r ': rs) ~ is'
    , s ~ r
    , s' ~ r'
    )
    => RecSubseq Rec (s ': ss) (s' ': ss') (r ': rs) (r' ': rs') (Z ': is') where
  rsubseqC f (r :& rs) =
      restore (:&) $ rsubseqC (keep runcons . f . (r :&)) rs
    where
      runcons :: Rec f (s' ': ss') -> (f s', Rec f ss')
      runcons (s' :& ss') = (s', ss')
  {-# inline rsubseqC #-}

  rsubseqSplitC = impossibleList


instance
    ( RecSubseq Rec ss ss' rs rs' (i ': dec_is')
    , DecSeq is' dec_is'
    , RImage ss (r ': rs) ~ IncSeq (RImage ss rs)
    , r ~ r'
    )
    => RecSubseq Rec ss ss' (r ': rs) (r' ': rs') (S i ': is') where
  rsubseqC f (r :& rs) =
      (r :&) <$> rsubseqC f rs
  {-# inline rsubseqC #-}

  rsubseqSplitC eq =
      L.withIso (rsubseqSplitC @Rec @ss eq) $ \split join ->
          L.iso (\(r :& rs)      -> second (r :&) (split rs))
                (\(ss, r :& rs') -> r :& join (ss, rs'))
  {-# inline rsubseqSplitC #-}


-- ARec instance

instance
    ( ReplaceSubseq ss ss' rs rs' is
    , NatToInt (RLength ss)
    , NatToInt (RLength rs)
    , NatToInt (RLength rs')
    , IndexWitnesses (RImage ss rs)
    )
    => RecSubseq ARec ss ss' rs rs' is where

  rsubseqC = L.lens rcast $ \(ARec ars) (ARec ass') ->
      let
        replace :: Int -> [Int] -> [Any] -> [Any] -> [Any]
        replace !_ _  rs []  = rs
        replace !_ _  [] ss' = ss'
        replace !_ [] rs ss' = ss' ++ rs

        replace !i jjs@(j : js) (r : rs) s'ss'@(s' : ss')
          | i == j    = s' : replace (i+1) js rs ss'
          | otherwise = r : replace (i+1) jjs rs s'ss'

        ars' = A.listArray (0, natToInt @(RLength rs') - 1) $
                replace 0 (indexWitnesses @is) (A.elems ars) (A.elems ass')
      in
        ARec ars'

  rsubseqSplitC E.Refl = L.iso
      (\(ARec ars) ->
          let
            split :: Int -> [Any] -> [Int] -> ([Any], [Any])
            split !_ rs [] = ([], rs)

            split !i (r : rs) jjs@(j : js)
              | i == j    = case split (i+1) rs js  of (!ss, !rs') -> (r : ss, rs')
              | otherwise = case split (i+1) rs jjs of (!ss, !rs') -> (ss, r : rs')

            split !_ [] (_:_) = error "rsubseqSplitC @ARec: split: the impossible happened"
          in
            case split 0 (A.elems ars) (indexWitnesses @is) of
              (ss, rs') -> (ARec $ A.listArray (0, natToInt @(RLength ss) - 1) ss,
                            ARec $ A.listArray (0, natToInt @(RLength rs') - 1) rs'))
      (\(ARec ass, ARec ars') ->
        let
          merge :: Int -> [Int] -> [Any] -> [Any] -> [Any]
          merge !_ [] [] rs' = rs'
          merge !_ _  ss []  = ss

          merge !i jjs@(j : js) sss@(s : ss) rrs'@(r : rs')
            | i == j    = s : merge (i+1) js ss rrs'
            | otherwise = r : merge (i+1) jjs sss rs'

          merge !_ [] (_:_) _ = error "rsubseqSplitC @ARec: merge: the impossible happened"
          merge !_ (_:_) [] _ = error "rsubseqSplitC @ARec: merge: the impossible happened"
        in
          ARec $ A.listArray (0, natToInt @(RLength rs) - 1) $
            merge 0 (indexWitnesses @is) (A.elems ass) (A.elems ars'))


type RSubseq ss ss' rs rs' = RecSubseq Rec ss ss' rs rs' (RImage ss rs)

type RecQuotient rec sub rs q = RecSubseq rec sub '[] rs q (RImage sub rs)
type RQuotient sub rs q = RecQuotient Rec sub rs q
