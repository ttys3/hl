use std::clone::Clone;
use std::collections::HashMap;
use std::vec::Vec;

use crate::eseq::{
    self, BasicColor::*, Brightness, Color, Flag, Flags, Instruction, Operator, ProcessSGR,
};
use crate::fmtx::Push;
use crate::settings;
use crate::types;

pub use types::Level;

// ---

pub trait StylingPush: Push<u8> {
    fn element<F: FnOnce(&mut Self)>(&mut self, element: Element, f: F);
}

// impl<S: StylingPush> StylingPush for &S {
//     type Next = S;
//     fn element<F: FnOnce(Self::Next)>(self, element: Element, f: F) {
//         StylingPush::element(*self, element, |s| f(&s));
//     }
// }

// ---

pub trait DirectBufAccess {
    fn buf_mut(&mut self) -> &mut Vec<u8>;
}

// impl<A: DirectBufAccess> DirectBufAccess for &mut A {
//     fn buf_mut(&mut self) -> &mut Vec<u8> {
//         DirectBufAccess::buf_mut(self)
//     }
// }

// ---

#[repr(u8)]
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

// ---

pub struct Styler<'a, P: ProcessSGR> {
    processor: &'a mut P,
    pack: &'a StylePack,
    current: Option<usize>,
}

impl<'a, P: ProcessSGR> Styler<'a, P> {
    pub fn set(&mut self, e: Element) {
        self.set_style(self.pack.elements[e as usize])
    }

    fn reset(&mut self) {
        self.set_style(None)
    }

    fn set_style(&mut self, style: Option<usize>) {
        let style = match style {
            Some(style) => Some(style),
            None => self.pack.reset,
        };
        if let Some(style) = style {
            if self.current != Some(style) {
                self.current = Some(style);
                let style = &self.pack.styles[style];
                style.apply(self.processor);
            }
        }
    }
}

impl<'a, P: ProcessSGR + 'a> StylingPush for Styler<'a, P> {
    fn element<F: FnOnce(&mut Self)>(&mut self, element: Element, f: F) {
        self.set(element);
        f(self);
        self.reset();
    }
}

impl<'a, P: ProcessSGR + 'a> Push<u8> for Styler<'a, P> {
    fn push(&mut self, data: u8) {
        Push::<u8>::push(self.processor, data)
    }
}

// ---

pub struct Theme {
    packs: HashMap<Level, StylePack>,
    default: StylePack,
}

impl Theme {
    pub fn none() -> Self {
        Self {
            packs: HashMap::new(),
            default: StylePack::none(),
        }
    }

    pub fn load(s: &settings::Theme) -> Self {
        let default = StylePack::load(&s.default);
        let mut packs = HashMap::new();
        for (level, pack) in &s.levels {
            packs.insert(*level, StylePack::load(&s.default.clone().merged(&pack)));
        }
        Self { default, packs }
    }

    pub fn apply<'a, P: ProcessSGR + 'a, F: FnOnce(&mut Styler<'a, P>)>(
        &'a self,
        processor: &'a mut P,
        level: &Option<Level>,
        f: F,
    ) {
        let mut styler = Styler {
            processor,
            pack: match level {
                Some(level) => match self.packs.get(level) {
                    Some(pack) => pack,
                    None => &self.default,
                },
                None => &self.default,
            },
            current: None,
        };
        f(&mut styler);
        styler.reset()
    }
}

// ---

#[derive(Clone, Default, Eq, PartialEq)]
struct Style(eseq::Style);

impl Style {
    pub fn apply<P: ProcessSGR>(&self, processor: &mut P) {
        if let Some(bg) = self.0.background {
            Push::<Instruction>::push(processor, Instruction::PushBackground(bg));
        }
        if let Some(fg) = self.0.foreground {
            Push::<Instruction>::push(processor, Instruction::PushForeground(fg));
        }
        if let Some((flags, annotations)) = self.0.flags {
            Push::<Instruction>::push(processor, Instruction::PushFlags(flags, annotations));
        }
    }

    pub fn revert<P: ProcessSGR>(&self, processor: &mut P) {
        if self.0.flags.is_some() {
            Push::<Instruction>::push(processor, Instruction::PopFlags);
        }
        if self.0.foreground.is_some() {
            Push::<Instruction>::push(processor, Instruction::PopForeground);
        }
        if self.0.background.is_some() {
            Push::<Instruction>::push(processor, Instruction::PopBackground);
        }
    }

    // pub fn reset() -> Self {
    //     Sequence::reset().into()
    // }

    fn convert_color(color: &settings::Color) -> Color {
        match color {
            settings::Color::Plain(color) => {
                let c = match color {
                    settings::PlainColor::Black => (Black, Brightness::Normal),
                    settings::PlainColor::Blue => (Blue, Brightness::Normal),
                    settings::PlainColor::Cyan => (Cyan, Brightness::Normal),
                    settings::PlainColor::Green => (Green, Brightness::Normal),
                    settings::PlainColor::Magenta => (Magenta, Brightness::Normal),
                    settings::PlainColor::Red => (Red, Brightness::Normal),
                    settings::PlainColor::White => (White, Brightness::Normal),
                    settings::PlainColor::Yellow => (Yellow, Brightness::Normal),
                    settings::PlainColor::BrightBlack => (Black, Brightness::Bright),
                    settings::PlainColor::BrightBlue => (Blue, Brightness::Bright),
                    settings::PlainColor::BrightCyan => (Cyan, Brightness::Bright),
                    settings::PlainColor::BrightGreen => (Green, Brightness::Bright),
                    settings::PlainColor::BrightMagenta => (Magenta, Brightness::Bright),
                    settings::PlainColor::BrightRed => (Red, Brightness::Bright),
                    settings::PlainColor::BrightWhite => (White, Brightness::Bright),
                    settings::PlainColor::BrightYellow => (Yellow, Brightness::Bright),
                };
                Color::Plain(c.0, c.1)
            }
            settings::Color::Palette(code) => Color::Palette(*code),
            settings::Color::RGB(settings::RGB(r, g, b)) => Color::RGB(*r, *g, *b),
        }
    }
}

// impl<T: Into<Sequence>> From<T> for Style {
//     fn from(value: T) -> Self {
//         Self(value.into())
//     }
// }

impl From<&settings::Style> for Style {
    fn from(style: &settings::Style) -> Self {
        let mut flags = Flags::none();
        for mode in &style.modes {
            flags |= match mode {
                settings::Mode::Bold => Flag::Bold,
                settings::Mode::Conceal => Flag::Concealed,
                settings::Mode::CrossedOut => Flag::CrossedOut,
                settings::Mode::Faint => Flag::Faint,
                settings::Mode::Italic => Flag::Italic,
                settings::Mode::RapidBlink => Flag::RapidBlink,
                settings::Mode::Reverse => Flag::Reversed,
                settings::Mode::SlowBlink => Flag::SlowBlink,
                settings::Mode::Underline => Flag::Underlined,
            };
        }
        let background = style.background.map(|color| Self::convert_color(&color));
        let foreground = style.foreground.map(|color| Self::convert_color(&color));
        Self(eseq::Style {
            flags: if flags.is_none() {
                None
            } else {
                Some((flags, Operator::Or))
            },
            background,
            foreground,
        })
    }
}

// ---

struct StylePack {
    elements: Vec<Option<usize>>,
    reset: Option<usize>,
    styles: Vec<Style>,
}

impl StylePack {
    fn new() -> Self {
        Self {
            styles: vec![Style::default()],
            reset: Some(0),
            elements: vec![None; 255],
        }
    }

    fn none() -> Self {
        Self {
            elements: vec![None; 255],
            reset: None,
            styles: Vec::new(),
        }
    }

    fn add(&mut self, element: Element, style: &Style) {
        let pos = match self.styles.iter().position(|x| x == style) {
            Some(pos) => pos,
            None => {
                self.styles.push(style.clone());
                self.styles.len() - 1
            }
        };
        self.elements[element as usize] = Some(pos);
    }

    fn load(s: &settings::StylePack<settings::Style>) -> Self {
        let mut result = Self::new();
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
        result.add(Element::Time, &Style::from(&s.time));
        result.add(Element::Whitespace, &Style::from(&s.time));
        result
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
        theme.apply(&mut buf, &Some(Level::Debug), |buf, styler| {
            styler.set(buf, Element::Message);
        });
    }
}
