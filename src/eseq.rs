// std imports
use std::{collections::HashMap, io::Write};

// third-party imports
use bitmask::bitmask;

// local imports
use crate::fmtx::Push;

// ---

bitmask! {
    #[derive(Debug,Default)]
    pub mask Flags: u16 where flags Flag {
        Bold                = 1 << 0,
        Faint               = 1 << 1,
        Italic              = 1 << 2,
        Underlined          = 1 << 3,
        SlowBlink           = 1 << 4,
        RapidBlink          = 1 << 5,
        Reversed            = 1 << 6,
        Concealed           = 1 << 7,
        CrossedOut          = 1 << 8,
        DoublyUnderlined    = 1 << 9,
        Framed              = 1 << 10,
        Encircled           = 1 << 11,
        Overlined           = 1 << 12,
        Superscript         = 1 << 13,
        Subscript           = 1 << 14,
    }
}

// ---

#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum CommandCode {
    ResetAll = 0,
    SetBold = 1,
    SetFaint = 2,
    SetItalic = 3,
    SetUnderlined = 4,
    SetSlowBlink = 5,
    SetRapidBlink = 6,
    SetReversed = 7,
    SetConcealed = 8,
    SetCrossedOut = 9,
    SetDoublyUnderlined = 21,
    ResetBoldAndFaint = 22,
    ResetItalic = 23,
    ResetAllUnderlines = 24,
    ResetAllBlinks = 25,
    ResetReversed = 27,
    ResetConcealed = 28,
    ResetCrossedOut = 29,
    SetFirstForegroundColor = 30,
    ResetForegroundColor = 39,
    SetFirstBackgroundColor = 40,
    ResetBackgroundColor = 49,
    SetFramed = 51,
    SetEncircled = 52,
    SetOverlined = 53,
    ResetFramedAndEncircled = 54,
    ResetOverlined = 55,
    SetUnderlineColor = 58,
    ResetUnderlineColor = 59,
    SetSuperscript = 73,
    SetSubscript = 74,
    ResetSuperscriptAndSubscript = 75,
    SetFirstBrightForegroundColor = 90,
    SetFirstBrightBackgroundColor = 100,
}

impl CommandCode {
    fn render(&self, buf: &mut Vec<u8>) {
        write!(buf, "{}", (*self as u8)).unwrap()
    }
}

// ---

#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum BasicColor {
    Black,
    Red,
    Green,
    Yellow,
    Blue,
    Magenta,
    Cyan,
    White,
}

impl BasicColor {
    pub fn bright(self) -> PlainColor {
        PlainColor(self, Brightness::Bright)
    }

    pub fn fg(self) -> (Instruction, Instruction) {
        Color::Plain(self, Brightness::Normal).fg()
    }

    pub fn bg(self) -> (Instruction, Instruction) {
        Color::Plain(self, Brightness::Normal).bg()
    }

    fn render(&self, buf: &mut Vec<u8>, base: u8) {
        write!(buf, "{}", base + (*self as u8)).unwrap()
    }
}

// ---

pub struct PlainColor(BasicColor, Brightness);

impl PlainColor {
    pub fn fg(self) -> (Instruction, Instruction) {
        Color::Plain(self.0, self.1).fg()
    }

    pub fn bg(self) -> (Instruction, Instruction) {
        Color::Plain(self.0, self.1).bg()
    }
}

// ---

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Color {
    Default,
    Plain(BasicColor, Brightness),
    Palette(u8),
    RGB(u8, u8, u8),
}

impl Color {
    pub fn fg(self) -> (Instruction, Instruction) {
        (
            Instruction::PushForeground(self),
            Instruction::PopForeground,
        )
    }

    pub fn bg(self) -> (Instruction, Instruction) {
        (
            Instruction::PushBackground(self),
            Instruction::PopBackground,
        )
    }

    fn render(&self, buf: &mut Vec<u8>, base: u8) {
        match self {
            Self::Default => write!(buf, "{}", base + 9).unwrap(),
            Self::Plain(color, Brightness::Normal) => color.render(buf, base),
            Self::Plain(color, Brightness::Bright) => color.render(buf, base + 60),
            Self::Palette(color) => write!(buf, "{};5;{}", base + 8, color).unwrap(),
            Self::RGB(r, g, b) => write!(buf, "{};2;{};{};{}", base + 8, r, g, b).unwrap(),
        }
    }
}

impl Default for Color {
    fn default() -> Self {
        Self::Default
    }
}

// ---

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Brightness {
    Normal,
    Bright,
}

// ---

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Command {
    Plain(CommandCode),
    SetBackground(Color),
    SetForeground(Color),
}

impl Command {
    fn render(&self, buf: &mut Vec<u8>) {
        match self {
            Self::Plain(code) => code.render(buf),
            Self::SetBackground(command) => command.render(buf, 40),
            Self::SetForeground(command) => command.render(buf, 30),
        }
    }
}

impl From<CommandCode> for Command {
    fn from(code: CommandCode) -> Self {
        Self::Plain(code)
    }
}

impl Into<Vec<u8>> for Command {
    fn into(self) -> Vec<u8> {
        let mut result = Vec::new();
        self.render(&mut result);
        result
    }
}

// ---

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Instruction {
    ResetAll,
    PushFlags(Flags, Operator),
    PopFlags,
    PushBackground(Color),
    PopBackground,
    PushForeground(Color),
    PopForeground,
}

// ---

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Operator {
    Set,
    And,
    Or,
    Xor,
}

// ---

#[derive(Clone, Eq, PartialEq)]
pub struct Sequence {
    buf: Vec<u8>,
}

impl Sequence {
    pub fn reset() -> Self {
        let mut buf = Vec::with_capacity(5);
        begin(&mut buf);
        end(&mut buf);
        Self { buf }
    }

    pub fn data(&self) -> &[u8] {
        &self.buf
    }
}

impl From<Vec<u8>> for Sequence {
    fn from(buf: Vec<u8>) -> Self {
        Self { buf }
    }
}

impl From<Vec<Command>> for Sequence {
    fn from(commands: Vec<Command>) -> Self {
        let mut buf = Vec::new();
        begin(&mut buf);
        for command in commands {
            next(&mut buf);
            command.render(&mut buf);
        }
        end(&mut buf);
        Self { buf }
    }
}

// ---

pub struct Processor<O: Push<u8>, const N: usize> {
    flags: State<Flags, N>,
    bg: State<Color, N>,
    fg: State<Color, N>,
    dirty: bool,
    cache: HashMap<Command, Vec<u8>>,
    output: O,
}

impl<O: Push<u8>, const N: usize> Processor<O, N> {
    pub fn new(output: O) -> Self {
        Self {
            flags: State::default(),
            bg: State::default(),
            fg: State::default(),
            dirty: false,
            cache: HashMap::new(),
            output,
        }
    }

    fn soil(&mut self) -> &mut Self {
        self.dirty = true;
        self
    }

    fn sync(&mut self) {
        if self.dirty {
            let bg = self.bg.stack.last().copied().unwrap_or_default();
            let fg = self.fg.stack.last().copied().unwrap_or_default();
            let flags = self.flags.stack.last().copied().unwrap_or_default();
            let mut first = true;
            let mut next = |output: &mut O, done: bool| {
                if done && !first {
                    output.extend_from_slice(if done {
                        END
                    } else if first {
                        BEGIN
                    } else {
                        NEXT
                    });
                    first = false;
                }
            };
            let cache = &mut self.cache;
            if self.bg.synced != bg {
                next(&mut self.output, false);
                self.output
                    .extend_from_slice(Self::cached(cache, Command::SetBackground(bg)));
                self.bg.synced = bg;
            }
            if self.bg.synced != fg {
                next(&mut self.output, false);
                self.output
                    .extend_from_slice(Self::cached(cache, Command::SetForeground(fg)));
                self.fg.synced = fg;
            }
            if self.flags.synced != flags {
                next(&mut self.output, false);
                let diff = self.flags.synced ^ flags;
                for (f0, f1, set0, set1, reset) in DUAL_SYNC_TABLE {
                    let actions = dual_flag_sync(diff, flags, *f0, *f1);
                    if actions.2 {
                        next(&mut self.output, false);
                        self.output
                            .extend_from_slice(Self::cached(cache, (*reset).into()));
                    }
                    if actions.0 {
                        next(&mut self.output, false);
                        self.output
                            .extend_from_slice(Self::cached(cache, (*set0).into()));
                    }
                    if actions.1 {
                        next(&mut self.output, false);
                        self.output
                            .extend_from_slice(Self::cached(cache, (*set1).into()));
                    }
                }
                for (f, set, reset) in SINGLE_SYNC_TABLE {
                    if diff.contains(*f) {
                        next(&mut self.output, false);
                        self.output.extend_from_slice(Self::cached(
                            cache,
                            if flags.contains(*f) { *set } else { *reset }.into(),
                        ));
                    }
                }
                self.flags.synced = flags;
            }
            next(&mut self.output, true);
            self.dirty = false;
        }
    }

    fn cached(cache: &mut HashMap<Command, Vec<u8>>, command: Command) -> &Vec<u8> {
        cache.entry(command).or_insert_with(|| command.into())
    }
}

impl<O: Push<u8>, const N: usize> Push<Instruction> for Processor<O, N> {
    #[inline]
    fn push(&mut self, instruction: Instruction) {
        match instruction {
            Instruction::ResetAll => {
                self.flags = State::default();
                self.bg = State::default();
                self.fg = State::default();
                self.output.extend_from_slice(BEGIN);
                self.output.extend_from_slice(END);
            }
            Instruction::PushFlags(flags, operator) => {
                let mut f = self.flags.stack.last().cloned().unwrap_or_default();
                match operator {
                    Operator::Set => f = flags,
                    Operator::And => f &= flags,
                    Operator::Or => f |= flags,
                    Operator::Xor => f ^= flags,
                };
                self.soil().flags.stack.push(f).unwrap();
            }
            Instruction::PopFlags => {
                self.soil().flags.stack.pop().unwrap();
            }
            Instruction::PushBackground(color) => {
                self.soil().bg.stack.push(color).unwrap();
            }
            Instruction::PopBackground => {
                self.soil().bg.stack.pop().unwrap();
            }
            Instruction::PushForeground(color) => {
                self.soil().fg.stack.push(color).unwrap();
            }
            Instruction::PopForeground => {
                self.soil().fg.stack.pop().unwrap();
            }
        }
    }
}

impl<O: Push<u8>, const N: usize> Push<u8> for Processor<O, N> {
    #[inline]
    fn push(&mut self, data: u8) {
        self.sync();
        self.output.push(data);
    }
    #[inline]
    fn extend_from_slice(&mut self, data: &[u8]) {
        self.sync();
        self.output.extend_from_slice(data);
    }
}

// ---

#[derive(Default)]
struct State<T: Copy, const N: usize> {
    synced: T,
    stack: heapless::Vec<T, N>,
}

// ---

const BEGIN: &[u8] = b"\x1b[";
const NEXT: &[u8] = b";";
const END: &[u8] = b"m";

const SINGLE_SYNC_TABLE: &[(Flag, CommandCode, CommandCode)] = &[
    (
        Flag::Italic,
        CommandCode::SetItalic,
        CommandCode::ResetItalic,
    ),
    (
        Flag::Concealed,
        CommandCode::SetConcealed,
        CommandCode::ResetConcealed,
    ),
    (
        Flag::CrossedOut,
        CommandCode::SetCrossedOut,
        CommandCode::ResetCrossedOut,
    ),
    (
        Flag::Reversed,
        CommandCode::SetReversed,
        CommandCode::ResetReversed,
    ),
    (
        Flag::Overlined,
        CommandCode::SetOverlined,
        CommandCode::ResetOverlined,
    ),
];

const DUAL_SYNC_TABLE: &[(Flag, Flag, CommandCode, CommandCode, CommandCode)] = &[
    (
        Flag::Bold,
        Flag::Faint,
        CommandCode::SetBold,
        CommandCode::SetFaint,
        CommandCode::ResetBoldAndFaint,
    ),
    (
        Flag::Underlined,
        Flag::DoublyUnderlined,
        CommandCode::SetUnderlined,
        CommandCode::SetDoublyUnderlined,
        CommandCode::ResetAllUnderlines,
    ),
    (
        Flag::SlowBlink,
        Flag::RapidBlink,
        CommandCode::SetSlowBlink,
        CommandCode::SetRapidBlink,
        CommandCode::ResetAllBlinks,
    ),
    (
        Flag::Framed,
        Flag::Encircled,
        CommandCode::SetFramed,
        CommandCode::SetEncircled,
        CommandCode::ResetFramedAndEncircled,
    ),
    (
        Flag::Subscript,
        Flag::Superscript,
        CommandCode::SetSubscript,
        CommandCode::SetSuperscript,
        CommandCode::ResetSuperscriptAndSubscript,
    ),
];

// ---

#[inline]
fn dual_flag_sync(mut diff: Flags, flags: Flags, f0: Flag, f1: Flag) -> (bool, bool, bool) {
    let mut result = (false, false, false);
    if !diff.intersects(f0 | f1) {
        return result;
    }
    if ((flags ^ diff) & diff).intersects(f0 | f1) {
        result.2 = true;
        diff |= flags & (f0 | f1);
    }
    if (diff & flags).contains(f0) {
        result.0 = true;
    }
    if (diff & flags).contains(f1) {
        result.1 = true;
    }
    result
}

#[inline]
fn begin(buf: &mut Vec<u8>) {
    buf.extend_from_slice(BEGIN);
}

#[inline]
fn next(buf: &mut Vec<u8>) {
    buf.extend_from_slice(NEXT);
}

#[inline]
fn end(buf: &mut Vec<u8>) {
    buf.extend_from_slice(END);
}

// ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dual_flag_sync() {
        //        CUR  NEW  CMD     CODE
        //   [0]  0/0  0/0           0
        //   [1]  0/0  0/1  S1       1
        //   [2]  0/0  1/0  S2       2
        //   [3]  0/0  1/1  S1,S2    1|2
        //   [4]  0/1  0/0  R        4
        //   [5]  0/1  0/1           0
        //   [6]  0/1  1/0  R,S2     4|2
        //   [7]  0/1  1/1  S2       2
        //   [8]  1/0  0/0  R        4
        //   [9]  1/0  0/1  R,S1     4|1
        //  [10]  1/0  1/0           0
        //  [11]  1/0  1/1  S1       1
        //  [12]  1/1  0/0  R        4
        //  [13]  1/1  0/1  R,S1     4|1
        //  [14]  1/1  1/0  R,S2     4|2
        //  [15]  1/1  1/1           0
        let table = [
            (Flags::none(), Flags::none(), (false, false, false)),
            (Flags::none(), Flag::Bold.into(), (true, false, false)),
            (Flags::none(), Flag::Faint.into(), (false, true, false)),
            (
                Flags::none(),
                (Flag::Bold | Flag::Faint).into(),
                (true, true, false),
            ),
            (Flag::Bold.into(), Flags::none(), (false, false, true)),
            (Flag::Bold.into(), Flag::Bold.into(), (false, false, false)),
            (Flag::Bold.into(), Flag::Faint.into(), (false, true, true)),
            (
                Flag::Bold.into(),
                (Flag::Bold | Flag::Faint).into(),
                (false, true, false),
            ),
            (Flag::Faint.into(), Flags::none(), (false, false, true)),
            (Flag::Faint.into(), Flag::Bold.into(), (true, false, true)),
            (
                Flag::Faint.into(),
                Flag::Faint.into(),
                (false, false, false),
            ),
            (
                Flag::Faint.into(),
                (Flag::Bold | Flag::Faint).into(),
                (true, false, false),
            ),
            (
                (Flag::Bold | Flag::Faint).into(),
                Flags::none(),
                (false, false, true),
            ),
            (
                (Flag::Bold | Flag::Faint).into(),
                Flag::Bold.into(),
                (true, false, true),
            ),
            (
                (Flag::Bold | Flag::Faint).into(),
                Flag::Faint.into(),
                (false, true, true),
            ),
            (
                (Flag::Bold | Flag::Faint).into(),
                (Flag::Bold | Flag::Faint).into(),
                (false, false, false),
            ),
        ];
        for (current, flags, expected) in table {
            let result = dual_flag_sync(current ^ flags, flags, Flag::Bold, Flag::Faint);
            assert_eq!(result, expected);
        }
    }
}
