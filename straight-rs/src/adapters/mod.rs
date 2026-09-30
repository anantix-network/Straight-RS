//! Conversions from Discord library types into Straight-RS's library-agnostic voice inputs.
#[cfg(feature = "serenity")]
pub mod serenity;
#[cfg(feature = "songbird")]
pub mod songbird;
#[cfg(feature = "twilight")]
pub mod twilight;
