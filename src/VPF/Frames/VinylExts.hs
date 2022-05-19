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

import Control.Lens (Iso, Iso', Lens)
import qualified Control.Lens as L

import Data.Bifunctor (first, second)
import qualified Data.Type.Equality as E

import Data.Vinyl (Rec(..), (<+>))

import Data.Vinyl.Functor (Compose(..))
import Data.Vinyl.TypeLevel


-- Class for any record that can be constructed from a single element

class RSingleton rec where
    rsingleton :: Iso (rec f '[a]) (rec f '[b]) (f a) (f b)

instance RSingleton Rec where
    rsingleton = L.iso (\(fa :& RNil) -> fa) (:& RNil)
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


type RSubseq ss ss' rs rs' = RecSubseq Rec ss ss' rs rs' (RImage ss rs)

type RecQuotient rec sub rs q = RecSubseq rec sub '[] rs q (RImage sub rs)
type RQuotient sub rs q = RecQuotient Rec sub rs q
