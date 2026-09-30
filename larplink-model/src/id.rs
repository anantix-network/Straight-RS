use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

macro_rules! id_type {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub u64);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt(f) }
        }
        impl From<u64> for $name {
            fn from(v: u64) -> Self { Self(v) }
        }
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(&self.0)
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct V;
                impl<'de> Visitor<'de> for V {
                    type Value = $name;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("a snowflake as string or unsigned integer")
                    }
                    fn visit_u64<E: de::Error>(self, v: u64) -> Result<$name, E> { Ok($name(v)) }
                    fn visit_i64<E: de::Error>(self, v: i64) -> Result<$name, E> {
                        u64::try_from(v).map($name).map_err(E::custom)
                    }
                    fn visit_str<E: de::Error>(self, v: &str) -> Result<$name, E> {
                        v.parse().map($name).map_err(E::custom)
                    }
                }
                d.deserialize_any(V)
            }
        }
    };
}

id_type!(/// Discord guild id.
    GuildId);
id_type!(/// Discord channel id.
    ChannelId);
id_type!(/// Discord user id.
    UserId);
