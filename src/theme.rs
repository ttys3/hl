// std imports
use std::{borrow::Borrow, vec::Vec};

// third-party imports
use enum_map::{Enum, EnumMap};
use platform_dirs::AppDirs;
use serde::Deserialize;

// local imports
use crate::{
    error::*,
    eseq::{Brightness, Color, ColorCode, Mode, Sequence, StyleCode},
    fmtx::Push,
    themecfg, types,
};
pub use types::Level;

// ---

pub trait StylingPush<B: Push<u8>> {
    fn element<F: FnOnce(&mut Self)>(&mut self, element: Element, f: F);
    fn batch<F: FnOnce(&mut B)>(&mut self, f: F);
}

// ---

#[repr(u8)]
#[derive(Enum, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Element {
    Time,
    Level,
    Logger,
    Caller,
    Message,
    EqualSign,
    Brace,
    Quote,
    Delimiter,
    Comma,
    AtSign,
    Ellipsis,
    FieldKey,
    Null,
    Boolean,
    Number,
    String,
    Whitespace,
}

pub type Buf = Vec<u8>;

pub struct Styler<'a, B: Push<u8>> {
    buf: &'a mut B,
    pack: &'a StylePack,
    current: Option<usize>,
}

pub struct Theme {
    packs: EnumMap<Level, StylePack>,
    default: StylePack,
}

#[derive(Clone, Eq, PartialEq)]
struct Style(Sequence);

impl Style {
    #[inline(always)]
    pub fn apply<B: Push<u8>>(&self, buf: &mut B) {
        buf.extend_from_slice(self.0.data())
    }

    pub fn reset() -> Self {
        Sequence::reset().into()
    }

    fn convert_color(color: &themecfg::Color) -> ColorCode {
        match color {
            themecfg::Color::Plain(color) => {
                let c = match color {
                    themecfg::PlainColor::Black => (Color::Black, Brightness::Normal),
                    themecfg::PlainColor::Blue => (Color::Blue, Brightness::Normal),
                    themecfg::PlainColor::Cyan => (Color::Cyan, Brightness::Normal),
                    themecfg::PlainColor::Green => (Color::Green, Brightness::Normal),
                    themecfg::PlainColor::Magenta => (Color::Magenta, Brightness::Normal),
                    themecfg::PlainColor::Red => (Color::Red, Brightness::Normal),
                    themecfg::PlainColor::White => (Color::White, Brightness::Normal),
                    themecfg::PlainColor::Yellow => (Color::Yellow, Brightness::Normal),
                    themecfg::PlainColor::BrightBlack => (Color::Black, Brightness::Bright),
                    themecfg::PlainColor::BrightBlue => (Color::Blue, Brightness::Bright),
                    themecfg::PlainColor::BrightCyan => (Color::Cyan, Brightness::Bright),
                    themecfg::PlainColor::BrightGreen => (Color::Green, Brightness::Bright),
                    themecfg::PlainColor::BrightMagenta => (Color::Magenta, Brightness::Bright),
                    themecfg::PlainColor::BrightRed => (Color::Red, Brightness::Bright),
                    themecfg::PlainColor::BrightWhite => (Color::White, Brightness::Bright),
                    themecfg::PlainColor::BrightYellow => (Color::Yellow, Brightness::Bright),
                };
                ColorCode::Plain(c.0, c.1)
            }
            themecfg::Color::Palette(code) => ColorCode::Palette(*code),
            themecfg::Color::RGB(themecfg::RGB(r, g, b)) => ColorCode::RGB(*r, *g, *b),
        }
    }
}

impl Default for Style {
    fn default() -> Self {
        Self::reset()
    }
}

impl<T: Into<Sequence>> From<T> for Style {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl From<&themecfg::Style> for Style {
    fn from(style: &themecfg::Style) -> Self {
        let mut codes = Vec::<StyleCode>::new();
        for mode in &style.modes {
            codes.push(
                match mode {
                    themecfg::Mode::Bold => Mode::Bold,
                    themecfg::Mode::Conseal => Mode::Conseal,
                    themecfg::Mode::CrossedOut => Mode::CrossedOut,
                    themecfg::Mode::Faint => Mode::Faint,
                    themecfg::Mode::Italic => Mode::Italic,
                    themecfg::Mode::RapidBlink => Mode::RapidBlink,
                    themecfg::Mode::Reverse => Mode::Reverse,
                    themecfg::Mode::SlowBlink => Mode::SlowBlink,
                    themecfg::Mode::Underline => Mode::Underline,
                }
                .into(),
            );
        }
        if let Some(color) = &style.background {
            codes.push(StyleCode::Background(Self::convert_color(color)));
        }
        if let Some(color) = &style.foreground {
            codes.push(StyleCode::Foreground(Self::convert_color(color)));
        }
        Self(codes.into())
    }
}

impl<'a, B: Push<u8>> Styler<'a, B> {
    #[inline(always)]
    pub fn set(&mut self, e: Element) {
        self.set_style(self.pack.elements[e])
    }

    #[inline(always)]
    fn reset(&mut self) {
        self.set_style(None)
    }

    #[inline(always)]
    fn set_style(&mut self, style: Option<usize>) {
        let style = match style {
            Some(style) => Some(style),
            None => self.pack.reset,
        };
        if let Some(style) = style {
            if self.current != Some(style) {
                self.current = Some(style);
                let style = &self.pack.styles[style];
                style.apply(self.buf);
            }
        }
    }
}

impl<'a, B: Push<u8>> StylingPush<B> for Styler<'a, B> {
    #[inline(always)]
    fn element<F: FnOnce(&mut Self)>(&mut self, element: Element, f: F) {
        self.set(element);
        f(self);
    }
    #[inline(always)]
    fn batch<F: FnOnce(&mut B)>(&mut self, f: F) {
        f(self.buf)
    }
}

impl Theme {
    pub fn apply<'a, B: Push<u8>, F: FnOnce(&mut Styler<'a, B>)>(
        &'a self,
        buf: &'a mut B,
        level: &Option<Level>,
        f: F,
    ) {
        let mut styler = Styler {
            buf,
            pack: match level {
                Some(level) => &self.packs[*level],
                None => &self.default,
            },
            current: None,
        };
        f(&mut styler);
        styler.reset()
    }
}

#[derive(Default)]
struct StylePack {
    elements: EnumMap<Element, Option<usize>>,
    reset: Option<usize>,
    styles: Vec<Style>,
}

impl StylePack {
    fn add(&mut self, element: Element, style: &Style) {
        let pos = match self.styles.iter().position(|x| x == style) {
            Some(pos) => pos,
            None => {
                self.styles.push(style.clone());
                self.styles.len() - 1
            }
        };
        self.elements[element] = Some(pos);
    }

    fn load(s: &themecfg::StylePack<themecfg::Style>) -> Self {
        let mut result = Self::default();
        result.add(Element::Caller, &Style::from(&s.caller));
        result.add(Element::Comma, &Style::from(&s.comma));
        result.add(Element::Delimiter, &Style::from(&s.delimiter));
        result.add(Element::Ellipsis, &Style::from(&s.ellipsis));
        result.add(Element::EqualSign, &Style::from(&s.equal_sign));
        result.add(Element::FieldKey, &Style::from(&s.field_key));
        result.add(Element::Level, &Style::from(&s.level));
        result.add(Element::Boolean, &Style::from(&s.boolean));
        result.add(Element::Null, &Style::from(&s.null));
        result.add(Element::Number, &Style::from(&s.number));
        result.add(Element::String, &Style::from(&s.string));
        result.add(Element::AtSign, &Style::from(&s.at_sign));
        result.add(Element::Logger, &Style::from(&s.logger));
        result.add(Element::Message, &Style::from(&s.message));
        result.add(Element::Quote, &Style::from(&s.quote));
        result.add(Element::Brace, &Style::from(&s.brace));
        result.add(Element::Time, &Style::from(&s.time));
        result.add(Element::Whitespace, &Style::from(&s.time));
        result
    }
}

// ---

impl Theme {
    pub fn none() -> Self {
        Self {
            packs: EnumMap::default(),
            default: StylePack::default(),
        }
    }

    pub fn load(app_dirs: &AppDirs, name: &str) -> Result<Self> {
        Ok(themecfg::Theme::load(app_dirs, name)?.into())
    }
}

impl<S: Borrow<themecfg::Theme>> From<S> for Theme {
    fn from(s: S) -> Self {
        let s = s.borrow();
        let default = StylePack::load(&s.default);
        let mut packs = EnumMap::default();
        for (level, pack) in &s.levels {
            packs[*level] = StylePack::load(&s.default.clone().merged(&pack));
        }
        Self { default, packs }
    }
}

// ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_theme() {
        let theme = Theme::none();
        let mut buf = Vec::new();
        theme.apply(&mut buf, &Some(Level::Debug), |s| {
            s.element(Element::Message, |s| {
                s.batch(|buf| buf.extend_from_slice(b"hello!"))
            });
        });
    }
}
