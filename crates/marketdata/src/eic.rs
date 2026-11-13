//! The delivery-area identity type: [`EicCode`], the compiled form of `delivery_areas.csv`
//! (the reference dump of Nord Pool's `delivery_area` feed, produced by
//! `nordpool-depth-replay --frame-type delivery-area`). The CSV is the data of record and a
//! unit test holds the enum to it exactly; when Nord Pool adds an area, regenerate the CSV
//! and append the variant (append-only — stored data decodes through this vocabulary). The
//! domain and every feed speak [`EicCode`]; the raw numeric
//! [`DeliveryAreaId`](crate::DeliveryAreaId) survives only at the PMD wire boundary
//! (subscriptions and inbound frames), converted via [`EicCode::id`] / [`EicCode::from_id`].

use crate::DeliveryAreaId;

/// A Nord Pool delivery area, identified by its ENTSO-E EIC code. Variants carry
/// Nord Pool's short area code verbatim behind an `AREA_` prefix; the EIC string and
/// the numeric PMD wire id ride along as associated data. Hardcoded, append-only, and
/// verified variant-for-variant against the bundled `delivery_areas.csv` (the data of
/// record) by a unit test — an area missing here means the reference CSV predates it:
/// regenerate the CSV from the archive and append the variant.
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EicCode {
    /// DK1 — `10YDK-1--------W` (id 1).
    AREA_DK1,
    /// DK2 — `10YDK-2--------M` (id 2).
    AREA_DK2,
    /// FI — `10YFI-1--------U` (id 3).
    AREA_FI,
    /// FRE — `10YDOM-1001A084H` (id 4).
    AREA_FRE,
    /// EE — `10Y1001A1001A39I` (id 5).
    AREA_EE,
    /// LV — `10YLV-1001A00074` (id 6).
    AREA_LV,
    /// LT — `10YLT-1001A0008Q` (id 7).
    AREA_LT,
    /// NO1 — `10YNO-1--------2` (id 8).
    AREA_NO1,
    /// NO2 — `10YNO-2--------T` (id 9).
    AREA_NO2,
    /// NO3 — `10YNO-3--------J` (id 10).
    AREA_NO3,
    /// NO4 — `10YNO-4--------9` (id 11).
    AREA_NO4,
    /// NO5 — `10Y1001A1001A48H` (id 12).
    AREA_NO5,
    /// SE1 — `10Y1001A1001A44P` (id 13).
    AREA_SE1,
    /// SE2 — `10Y1001A1001A45N` (id 14).
    AREA_SE2,
    /// SE3 — `10Y1001A1001A46L` (id 15).
    AREA_SE3,
    /// SE4 — `10Y1001A1001A47J` (id 16).
    AREA_SE4,
    /// CNOR — `10Y1001A1001A70O` (id 18).
    AREA_CNOR,
    /// COAC — `10Y1001A1001A885` (id 19).
    AREA_COAC,
    /// CORS — `10Y1001A1001A893` (id 20).
    AREA_CORS,
    /// CSUD — `10Y1001A1001A71M` (id 21).
    AREA_CSUD,
    /// MALTA — `10Y1001A1001A877` (id 23).
    AREA_MALTA,
    /// NORD — `10Y1001A1001A73I` (id 24).
    AREA_NORD,
    /// SARD — `10Y1001A1001A74G` (id 27).
    AREA_SARD,
    /// SICI — `10Y1001A1001A75E` (id 28).
    AREA_SICI,
    /// SUD — `10Y1001A1001A788` (id 29).
    AREA_SUD,
    /// PT — `10YPT-REN------W` (id 30).
    AREA_PT,
    /// ES — `10YES-REE------0` (id 31).
    AREA_ES,
    /// MO — `10YMA-ONE------O` (id 32).
    AREA_MO,
    /// UK — `10Y1001A1001A57G` (id 33).
    AREA_UK,
    /// NL — `10YNL----------L` (id 34).
    AREA_NL,
    /// BE — `10YBE----------2` (id 35).
    AREA_BE,
    /// TBW — `10YDE-ENBW-----N` (id 36).
    AREA_TBW,
    /// AMP — `10YDE-RWENET---I` (id 37).
    AREA_AMP,
    /// TTG — `10YDE-EON------1` (id 38).
    AREA_TTG,
    /// 50Hz — `10YDE-VE-------2` (id 39).
    AREA_50HZ,
    /// FR — `10YFR-RTE------C` (id 40).
    AREA_FR,
    /// CH — `10YCH-SWISSGRIDZ` (id 41).
    AREA_CH,
    /// AT — `10YAT-APG------L` (id 42).
    AREA_AT,
    /// DK1A — `10YDK-1-------AA` (id 43).
    AREA_DK1A,
    /// NO1A — `10Y1001A1001A64J` (id 44).
    AREA_NO1A,
    /// HR — `10YHR-HEP------M` (id 45).
    AREA_HR,
    /// BG — `10YCA-BULGARIA-R` (id 46).
    AREA_BG,
    /// IE — `10Y1001A1001A59C` (id 86).
    AREA_IE,
    /// PL — `10YPL-AREA-----S` (id 88).
    AREA_PL,
    /// GB2 — `10Y1001A1001A58E` (id 90).
    AREA_GB2,
    /// ELE — `38YNPSLVEXP----Y` (id 92).
    AREA_ELE,
    /// ELI — `38YNPSLVIMP----5` (id 93).
    AREA_ELI,
    /// ERI — `38YNPSRUIMP----S` (id 94).
    AREA_ERI,
    /// GER — `10YCB-GERMANY--8` (id 95).
    AREA_GER,
    /// LBE — `10Y1001A1001A56I` (id 96).
    AREA_LBE,
    /// LBI — `10Y1001A1001A55K` (id 97).
    AREA_LBI,
    /// LRE — `43YLRE-------008` (id 98).
    AREA_LRE,
    /// LRI — `43YLRI--------04` (id 99).
    AREA_LRI,
    /// PLA — `10YDOM-PL-SE-LT2` (id 100).
    AREA_PLA,
    /// ELES — `10YSI-ELES-----O` (id 108).
    AREA_ELES,
    /// CEPS — `10YCZ-CEPS-----N` (id 109).
    AREA_CEPS,
    /// MAVIR — `10YHU-MAVIR----U` (id 110).
    AREA_MAVIR,
    /// TEL — `10YRO-TEL------P` (id 111).
    AREA_TEL,
    /// GE — `10Y1001A1001B012` (id 113).
    AREA_GE,
    /// GB1A — `11Y0-0000-0265-K` (id 114).
    AREA_GB1A,
    /// PLC — `10Y1001C--00038X` (id 115).
    AREA_PLC,
    /// ALBE — `22Y201903144---9` (id 116).
    AREA_ALBE,
    /// ALDE — `22Y201903145---4` (id 117).
    AREA_ALDE,
    /// ITCALA — `10Y1001C--00096J` (id 118).
    AREA_ITCALA,
    /// ITFR — `10Y1001A1001A81J` (id 120).
    AREA_ITFR,
    /// ITCH — `10Y1001A1001A68B` (id 122).
    AREA_ITCH,
    /// ITAT — `10Y1001A1001A80L` (id 124).
    AREA_ITAT,
    /// ITSI — `10Y1001A1001A67D` (id 126).
    AREA_ITSI,
    /// ITME — `10Y1001C--000611` (id 128).
    AREA_ITME,
    /// ITGR — `10Y1001A1001A66F` (id 130).
    AREA_ITGR,
    /// ITCP — `10YIT-GRTN-----B` (id 132).
    AREA_ITCP,
    /// DEA2 — `10YDOM-1001A082L` (id 134).
    AREA_DEA2,
    /// SK — `10YSK-SEPS-----K` (id 135).
    AREA_SK,
    /// GREE — `10YGR-HTSO-----Y` (id 136).
    AREA_GREE,
    /// NO2A — `10Y1001C--001219` (id 137).
    AREA_NO2A,
    /// LKAL — `10Y1001A1001A50U` (id 138).
    AREA_LKAL,
    /// DE-LU — `10Y1001A1001A82H` (id 139).
    AREA_DE_LU,
    /// BY — `10Y1001A1001A51S` (id 140).
    AREA_BY,
    /// RU — `10Y1001A1001A49F` (id 141).
    AREA_RU,
    /// GB — `10YGB----------A` (id 142).
    AREA_GB,
    /// NO2_SK — `50YCUY85S1HH29EK` (id 143).
    AREA_NO2_SK,
    /// DK1_SK — `45Y000000000001C` (id 144).
    AREA_DK1_SK,
    /// DK1_SB — `45Y0000000000038` (id 145).
    AREA_DK1_SB,
    /// DK2_SB — `45Y0000000000062` (id 146).
    AREA_DK2_SB,
    /// FI_FS — `44Y-00000000160K` (id 147).
    AREA_FI_FS,
    /// SE3_FS — `46Y000000000001Y` (id 148).
    AREA_SE3_FS,
    /// DK1_KS — `45Y000000000002A` (id 149).
    AREA_DK1_KS,
    /// SE3_KS — `46Y000000000002W` (id 150).
    AREA_SE3_KS,
    /// SE4_SP — `46Y000000000003U` (id 151).
    AREA_SE4_SP,
    /// SE4_NB — `46Y000000000004S` (id 152).
    AREA_SE4_NB,
    /// SE4_BC — `46Y000000000005Q` (id 153).
    AREA_SE4_BC,
    /// FI_EL — `44Y-00000000161I` (id 154).
    AREA_FI_EL,
    /// DK1_DE — `45Y0000000000054` (id 155).
    AREA_DK1_DE,
    /// DK2_KO — `45Y0000000000070` (id 156).
    AREA_DK2_KO,
    /// DK1_CO — `45Y0000000000046` (id 157).
    AREA_DK1_CO,
    /// NO2_ND — `50Y73EMZ34CQL9AJ` (id 158).
    AREA_NO2_ND,
    /// NO2_NK — `50YNBFFTWZRAHA3P` (id 159).
    AREA_NO2_NK,
    /// SE3_SWL — `46Y000000000017J` (id 160).
    AREA_SE3_SWL,
    /// SE4_SWL — `46Y000000000018H` (id 161).
    AREA_SE4_SWL,
    /// IS — `10Y1001A1001A958` (id 162).
    AREA_IS,
    /// UA-IPS — `10Y1001C--000182` (id 172).
    AREA_UA_IPS,
    /// LU — `10YLU-CEGEDEL-NQ` (id 173).
    AREA_LU,
    /// MD — `10Y1001A1001A990` (id 174).
    AREA_MD,
    /// RS — `10YCS-SERBIATSOV` (id 175).
    AREA_RS,
    /// UA-BEI — `10YUA-WEPS-----0` (id 177).
    AREA_UA_BEI,
    /// TR — `10YTR-TEIAS----W` (id 178).
    AREA_TR,
    /// MK — `10YMK-MEPSO----8` (id 179).
    AREA_MK,
    /// SE3A — `10Y1001C--00148Q` (id 180).
    AREA_SE3A,
    /// DE_DK1 — `10YDK1-DE-10010E` (id 181).
    AREA_DE_DK1,
    /// DE_KO — `10Y1001C--001510` (id 182).
    AREA_DE_KO,
    /// DE_NK — `10Y1001C--00152Z` (id 183).
    AREA_DE_NK,
    /// DE_BC — `10YDE-SE4-VBZ-01` (id 184).
    AREA_DE_BC,
    /// NL_CO — `49Y000000000003M` (id 185).
    AREA_NL_CO,
    /// NL_ND — `49Y000000000004K` (id 186).
    AREA_NL_ND,
    /// PL_LP — `10Y1001C--00153X` (id 187).
    AREA_PL_LP,
    /// PL_SP — `10Y1001C--00154V` (id 188).
    AREA_PL_SP,
    /// RO_BG — `10Y1001C--00158N` (id 189).
    AREA_RO_BG,
}

impl EicCode {
    /// Every known delivery area, ordered by wire id.
    pub const ALL: &'static [EicCode] = &[
        EicCode::AREA_DK1,
        EicCode::AREA_DK2,
        EicCode::AREA_FI,
        EicCode::AREA_FRE,
        EicCode::AREA_EE,
        EicCode::AREA_LV,
        EicCode::AREA_LT,
        EicCode::AREA_NO1,
        EicCode::AREA_NO2,
        EicCode::AREA_NO3,
        EicCode::AREA_NO4,
        EicCode::AREA_NO5,
        EicCode::AREA_SE1,
        EicCode::AREA_SE2,
        EicCode::AREA_SE3,
        EicCode::AREA_SE4,
        EicCode::AREA_CNOR,
        EicCode::AREA_COAC,
        EicCode::AREA_CORS,
        EicCode::AREA_CSUD,
        EicCode::AREA_MALTA,
        EicCode::AREA_NORD,
        EicCode::AREA_SARD,
        EicCode::AREA_SICI,
        EicCode::AREA_SUD,
        EicCode::AREA_PT,
        EicCode::AREA_ES,
        EicCode::AREA_MO,
        EicCode::AREA_UK,
        EicCode::AREA_NL,
        EicCode::AREA_BE,
        EicCode::AREA_TBW,
        EicCode::AREA_AMP,
        EicCode::AREA_TTG,
        EicCode::AREA_50HZ,
        EicCode::AREA_FR,
        EicCode::AREA_CH,
        EicCode::AREA_AT,
        EicCode::AREA_DK1A,
        EicCode::AREA_NO1A,
        EicCode::AREA_HR,
        EicCode::AREA_BG,
        EicCode::AREA_IE,
        EicCode::AREA_PL,
        EicCode::AREA_GB2,
        EicCode::AREA_ELE,
        EicCode::AREA_ELI,
        EicCode::AREA_ERI,
        EicCode::AREA_GER,
        EicCode::AREA_LBE,
        EicCode::AREA_LBI,
        EicCode::AREA_LRE,
        EicCode::AREA_LRI,
        EicCode::AREA_PLA,
        EicCode::AREA_ELES,
        EicCode::AREA_CEPS,
        EicCode::AREA_MAVIR,
        EicCode::AREA_TEL,
        EicCode::AREA_GE,
        EicCode::AREA_GB1A,
        EicCode::AREA_PLC,
        EicCode::AREA_ALBE,
        EicCode::AREA_ALDE,
        EicCode::AREA_ITCALA,
        EicCode::AREA_ITFR,
        EicCode::AREA_ITCH,
        EicCode::AREA_ITAT,
        EicCode::AREA_ITSI,
        EicCode::AREA_ITME,
        EicCode::AREA_ITGR,
        EicCode::AREA_ITCP,
        EicCode::AREA_DEA2,
        EicCode::AREA_SK,
        EicCode::AREA_GREE,
        EicCode::AREA_NO2A,
        EicCode::AREA_LKAL,
        EicCode::AREA_DE_LU,
        EicCode::AREA_BY,
        EicCode::AREA_RU,
        EicCode::AREA_GB,
        EicCode::AREA_NO2_SK,
        EicCode::AREA_DK1_SK,
        EicCode::AREA_DK1_SB,
        EicCode::AREA_DK2_SB,
        EicCode::AREA_FI_FS,
        EicCode::AREA_SE3_FS,
        EicCode::AREA_DK1_KS,
        EicCode::AREA_SE3_KS,
        EicCode::AREA_SE4_SP,
        EicCode::AREA_SE4_NB,
        EicCode::AREA_SE4_BC,
        EicCode::AREA_FI_EL,
        EicCode::AREA_DK1_DE,
        EicCode::AREA_DK2_KO,
        EicCode::AREA_DK1_CO,
        EicCode::AREA_NO2_ND,
        EicCode::AREA_NO2_NK,
        EicCode::AREA_SE3_SWL,
        EicCode::AREA_SE4_SWL,
        EicCode::AREA_IS,
        EicCode::AREA_UA_IPS,
        EicCode::AREA_LU,
        EicCode::AREA_MD,
        EicCode::AREA_RS,
        EicCode::AREA_UA_BEI,
        EicCode::AREA_TR,
        EicCode::AREA_MK,
        EicCode::AREA_SE3A,
        EicCode::AREA_DE_DK1,
        EicCode::AREA_DE_KO,
        EicCode::AREA_DE_NK,
        EicCode::AREA_DE_BC,
        EicCode::AREA_NL_CO,
        EicCode::AREA_NL_ND,
        EicCode::AREA_PL_LP,
        EicCode::AREA_PL_SP,
        EicCode::AREA_RO_BG,
    ];

    /// The numeric delivery-area id used on the PMD wire (subscriptions, frames).
    pub const fn id(self) -> DeliveryAreaId {
        match self {
            EicCode::AREA_DK1 => 1,
            EicCode::AREA_DK2 => 2,
            EicCode::AREA_FI => 3,
            EicCode::AREA_FRE => 4,
            EicCode::AREA_EE => 5,
            EicCode::AREA_LV => 6,
            EicCode::AREA_LT => 7,
            EicCode::AREA_NO1 => 8,
            EicCode::AREA_NO2 => 9,
            EicCode::AREA_NO3 => 10,
            EicCode::AREA_NO4 => 11,
            EicCode::AREA_NO5 => 12,
            EicCode::AREA_SE1 => 13,
            EicCode::AREA_SE2 => 14,
            EicCode::AREA_SE3 => 15,
            EicCode::AREA_SE4 => 16,
            EicCode::AREA_CNOR => 18,
            EicCode::AREA_COAC => 19,
            EicCode::AREA_CORS => 20,
            EicCode::AREA_CSUD => 21,
            EicCode::AREA_MALTA => 23,
            EicCode::AREA_NORD => 24,
            EicCode::AREA_SARD => 27,
            EicCode::AREA_SICI => 28,
            EicCode::AREA_SUD => 29,
            EicCode::AREA_PT => 30,
            EicCode::AREA_ES => 31,
            EicCode::AREA_MO => 32,
            EicCode::AREA_UK => 33,
            EicCode::AREA_NL => 34,
            EicCode::AREA_BE => 35,
            EicCode::AREA_TBW => 36,
            EicCode::AREA_AMP => 37,
            EicCode::AREA_TTG => 38,
            EicCode::AREA_50HZ => 39,
            EicCode::AREA_FR => 40,
            EicCode::AREA_CH => 41,
            EicCode::AREA_AT => 42,
            EicCode::AREA_DK1A => 43,
            EicCode::AREA_NO1A => 44,
            EicCode::AREA_HR => 45,
            EicCode::AREA_BG => 46,
            EicCode::AREA_IE => 86,
            EicCode::AREA_PL => 88,
            EicCode::AREA_GB2 => 90,
            EicCode::AREA_ELE => 92,
            EicCode::AREA_ELI => 93,
            EicCode::AREA_ERI => 94,
            EicCode::AREA_GER => 95,
            EicCode::AREA_LBE => 96,
            EicCode::AREA_LBI => 97,
            EicCode::AREA_LRE => 98,
            EicCode::AREA_LRI => 99,
            EicCode::AREA_PLA => 100,
            EicCode::AREA_ELES => 108,
            EicCode::AREA_CEPS => 109,
            EicCode::AREA_MAVIR => 110,
            EicCode::AREA_TEL => 111,
            EicCode::AREA_GE => 113,
            EicCode::AREA_GB1A => 114,
            EicCode::AREA_PLC => 115,
            EicCode::AREA_ALBE => 116,
            EicCode::AREA_ALDE => 117,
            EicCode::AREA_ITCALA => 118,
            EicCode::AREA_ITFR => 120,
            EicCode::AREA_ITCH => 122,
            EicCode::AREA_ITAT => 124,
            EicCode::AREA_ITSI => 126,
            EicCode::AREA_ITME => 128,
            EicCode::AREA_ITGR => 130,
            EicCode::AREA_ITCP => 132,
            EicCode::AREA_DEA2 => 134,
            EicCode::AREA_SK => 135,
            EicCode::AREA_GREE => 136,
            EicCode::AREA_NO2A => 137,
            EicCode::AREA_LKAL => 138,
            EicCode::AREA_DE_LU => 139,
            EicCode::AREA_BY => 140,
            EicCode::AREA_RU => 141,
            EicCode::AREA_GB => 142,
            EicCode::AREA_NO2_SK => 143,
            EicCode::AREA_DK1_SK => 144,
            EicCode::AREA_DK1_SB => 145,
            EicCode::AREA_DK2_SB => 146,
            EicCode::AREA_FI_FS => 147,
            EicCode::AREA_SE3_FS => 148,
            EicCode::AREA_DK1_KS => 149,
            EicCode::AREA_SE3_KS => 150,
            EicCode::AREA_SE4_SP => 151,
            EicCode::AREA_SE4_NB => 152,
            EicCode::AREA_SE4_BC => 153,
            EicCode::AREA_FI_EL => 154,
            EicCode::AREA_DK1_DE => 155,
            EicCode::AREA_DK2_KO => 156,
            EicCode::AREA_DK1_CO => 157,
            EicCode::AREA_NO2_ND => 158,
            EicCode::AREA_NO2_NK => 159,
            EicCode::AREA_SE3_SWL => 160,
            EicCode::AREA_SE4_SWL => 161,
            EicCode::AREA_IS => 162,
            EicCode::AREA_UA_IPS => 172,
            EicCode::AREA_LU => 173,
            EicCode::AREA_MD => 174,
            EicCode::AREA_RS => 175,
            EicCode::AREA_UA_BEI => 177,
            EicCode::AREA_TR => 178,
            EicCode::AREA_MK => 179,
            EicCode::AREA_SE3A => 180,
            EicCode::AREA_DE_DK1 => 181,
            EicCode::AREA_DE_KO => 182,
            EicCode::AREA_DE_NK => 183,
            EicCode::AREA_DE_BC => 184,
            EicCode::AREA_NL_CO => 185,
            EicCode::AREA_NL_ND => 186,
            EicCode::AREA_PL_LP => 187,
            EicCode::AREA_PL_SP => 188,
            EicCode::AREA_RO_BG => 189,
        }
    }

    /// The ENTSO-E EIC code, e.g. `10YFI-1--------U`.
    pub const fn eic_code(self) -> &'static str {
        match self {
            EicCode::AREA_DK1 => "10YDK-1--------W",
            EicCode::AREA_DK2 => "10YDK-2--------M",
            EicCode::AREA_FI => "10YFI-1--------U",
            EicCode::AREA_FRE => "10YDOM-1001A084H",
            EicCode::AREA_EE => "10Y1001A1001A39I",
            EicCode::AREA_LV => "10YLV-1001A00074",
            EicCode::AREA_LT => "10YLT-1001A0008Q",
            EicCode::AREA_NO1 => "10YNO-1--------2",
            EicCode::AREA_NO2 => "10YNO-2--------T",
            EicCode::AREA_NO3 => "10YNO-3--------J",
            EicCode::AREA_NO4 => "10YNO-4--------9",
            EicCode::AREA_NO5 => "10Y1001A1001A48H",
            EicCode::AREA_SE1 => "10Y1001A1001A44P",
            EicCode::AREA_SE2 => "10Y1001A1001A45N",
            EicCode::AREA_SE3 => "10Y1001A1001A46L",
            EicCode::AREA_SE4 => "10Y1001A1001A47J",
            EicCode::AREA_CNOR => "10Y1001A1001A70O",
            EicCode::AREA_COAC => "10Y1001A1001A885",
            EicCode::AREA_CORS => "10Y1001A1001A893",
            EicCode::AREA_CSUD => "10Y1001A1001A71M",
            EicCode::AREA_MALTA => "10Y1001A1001A877",
            EicCode::AREA_NORD => "10Y1001A1001A73I",
            EicCode::AREA_SARD => "10Y1001A1001A74G",
            EicCode::AREA_SICI => "10Y1001A1001A75E",
            EicCode::AREA_SUD => "10Y1001A1001A788",
            EicCode::AREA_PT => "10YPT-REN------W",
            EicCode::AREA_ES => "10YES-REE------0",
            EicCode::AREA_MO => "10YMA-ONE------O",
            EicCode::AREA_UK => "10Y1001A1001A57G",
            EicCode::AREA_NL => "10YNL----------L",
            EicCode::AREA_BE => "10YBE----------2",
            EicCode::AREA_TBW => "10YDE-ENBW-----N",
            EicCode::AREA_AMP => "10YDE-RWENET---I",
            EicCode::AREA_TTG => "10YDE-EON------1",
            EicCode::AREA_50HZ => "10YDE-VE-------2",
            EicCode::AREA_FR => "10YFR-RTE------C",
            EicCode::AREA_CH => "10YCH-SWISSGRIDZ",
            EicCode::AREA_AT => "10YAT-APG------L",
            EicCode::AREA_DK1A => "10YDK-1-------AA",
            EicCode::AREA_NO1A => "10Y1001A1001A64J",
            EicCode::AREA_HR => "10YHR-HEP------M",
            EicCode::AREA_BG => "10YCA-BULGARIA-R",
            EicCode::AREA_IE => "10Y1001A1001A59C",
            EicCode::AREA_PL => "10YPL-AREA-----S",
            EicCode::AREA_GB2 => "10Y1001A1001A58E",
            EicCode::AREA_ELE => "38YNPSLVEXP----Y",
            EicCode::AREA_ELI => "38YNPSLVIMP----5",
            EicCode::AREA_ERI => "38YNPSRUIMP----S",
            EicCode::AREA_GER => "10YCB-GERMANY--8",
            EicCode::AREA_LBE => "10Y1001A1001A56I",
            EicCode::AREA_LBI => "10Y1001A1001A55K",
            EicCode::AREA_LRE => "43YLRE-------008",
            EicCode::AREA_LRI => "43YLRI--------04",
            EicCode::AREA_PLA => "10YDOM-PL-SE-LT2",
            EicCode::AREA_ELES => "10YSI-ELES-----O",
            EicCode::AREA_CEPS => "10YCZ-CEPS-----N",
            EicCode::AREA_MAVIR => "10YHU-MAVIR----U",
            EicCode::AREA_TEL => "10YRO-TEL------P",
            EicCode::AREA_GE => "10Y1001A1001B012",
            EicCode::AREA_GB1A => "11Y0-0000-0265-K",
            EicCode::AREA_PLC => "10Y1001C--00038X",
            EicCode::AREA_ALBE => "22Y201903144---9",
            EicCode::AREA_ALDE => "22Y201903145---4",
            EicCode::AREA_ITCALA => "10Y1001C--00096J",
            EicCode::AREA_ITFR => "10Y1001A1001A81J",
            EicCode::AREA_ITCH => "10Y1001A1001A68B",
            EicCode::AREA_ITAT => "10Y1001A1001A80L",
            EicCode::AREA_ITSI => "10Y1001A1001A67D",
            EicCode::AREA_ITME => "10Y1001C--000611",
            EicCode::AREA_ITGR => "10Y1001A1001A66F",
            EicCode::AREA_ITCP => "10YIT-GRTN-----B",
            EicCode::AREA_DEA2 => "10YDOM-1001A082L",
            EicCode::AREA_SK => "10YSK-SEPS-----K",
            EicCode::AREA_GREE => "10YGR-HTSO-----Y",
            EicCode::AREA_NO2A => "10Y1001C--001219",
            EicCode::AREA_LKAL => "10Y1001A1001A50U",
            EicCode::AREA_DE_LU => "10Y1001A1001A82H",
            EicCode::AREA_BY => "10Y1001A1001A51S",
            EicCode::AREA_RU => "10Y1001A1001A49F",
            EicCode::AREA_GB => "10YGB----------A",
            EicCode::AREA_NO2_SK => "50YCUY85S1HH29EK",
            EicCode::AREA_DK1_SK => "45Y000000000001C",
            EicCode::AREA_DK1_SB => "45Y0000000000038",
            EicCode::AREA_DK2_SB => "45Y0000000000062",
            EicCode::AREA_FI_FS => "44Y-00000000160K",
            EicCode::AREA_SE3_FS => "46Y000000000001Y",
            EicCode::AREA_DK1_KS => "45Y000000000002A",
            EicCode::AREA_SE3_KS => "46Y000000000002W",
            EicCode::AREA_SE4_SP => "46Y000000000003U",
            EicCode::AREA_SE4_NB => "46Y000000000004S",
            EicCode::AREA_SE4_BC => "46Y000000000005Q",
            EicCode::AREA_FI_EL => "44Y-00000000161I",
            EicCode::AREA_DK1_DE => "45Y0000000000054",
            EicCode::AREA_DK2_KO => "45Y0000000000070",
            EicCode::AREA_DK1_CO => "45Y0000000000046",
            EicCode::AREA_NO2_ND => "50Y73EMZ34CQL9AJ",
            EicCode::AREA_NO2_NK => "50YNBFFTWZRAHA3P",
            EicCode::AREA_SE3_SWL => "46Y000000000017J",
            EicCode::AREA_SE4_SWL => "46Y000000000018H",
            EicCode::AREA_IS => "10Y1001A1001A958",
            EicCode::AREA_UA_IPS => "10Y1001C--000182",
            EicCode::AREA_LU => "10YLU-CEGEDEL-NQ",
            EicCode::AREA_MD => "10Y1001A1001A990",
            EicCode::AREA_RS => "10YCS-SERBIATSOV",
            EicCode::AREA_UA_BEI => "10YUA-WEPS-----0",
            EicCode::AREA_TR => "10YTR-TEIAS----W",
            EicCode::AREA_MK => "10YMK-MEPSO----8",
            EicCode::AREA_SE3A => "10Y1001C--00148Q",
            EicCode::AREA_DE_DK1 => "10YDK1-DE-10010E",
            EicCode::AREA_DE_KO => "10Y1001C--001510",
            EicCode::AREA_DE_NK => "10Y1001C--00152Z",
            EicCode::AREA_DE_BC => "10YDE-SE4-VBZ-01",
            EicCode::AREA_NL_CO => "49Y000000000003M",
            EicCode::AREA_NL_ND => "49Y000000000004K",
            EicCode::AREA_PL_LP => "10Y1001C--00153X",
            EicCode::AREA_PL_SP => "10Y1001C--00154V",
            EicCode::AREA_RO_BG => "10Y1001C--00158N",
        }
    }

    /// Nord Pool's short human-readable area code, e.g. `FI` — for logs and labels.
    pub const fn area_code(self) -> &'static str {
        match self {
            EicCode::AREA_DK1 => "DK1",
            EicCode::AREA_DK2 => "DK2",
            EicCode::AREA_FI => "FI",
            EicCode::AREA_FRE => "FRE",
            EicCode::AREA_EE => "EE",
            EicCode::AREA_LV => "LV",
            EicCode::AREA_LT => "LT",
            EicCode::AREA_NO1 => "NO1",
            EicCode::AREA_NO2 => "NO2",
            EicCode::AREA_NO3 => "NO3",
            EicCode::AREA_NO4 => "NO4",
            EicCode::AREA_NO5 => "NO5",
            EicCode::AREA_SE1 => "SE1",
            EicCode::AREA_SE2 => "SE2",
            EicCode::AREA_SE3 => "SE3",
            EicCode::AREA_SE4 => "SE4",
            EicCode::AREA_CNOR => "CNOR",
            EicCode::AREA_COAC => "COAC",
            EicCode::AREA_CORS => "CORS",
            EicCode::AREA_CSUD => "CSUD",
            EicCode::AREA_MALTA => "MALTA",
            EicCode::AREA_NORD => "NORD",
            EicCode::AREA_SARD => "SARD",
            EicCode::AREA_SICI => "SICI",
            EicCode::AREA_SUD => "SUD",
            EicCode::AREA_PT => "PT",
            EicCode::AREA_ES => "ES",
            EicCode::AREA_MO => "MO",
            EicCode::AREA_UK => "UK",
            EicCode::AREA_NL => "NL",
            EicCode::AREA_BE => "BE",
            EicCode::AREA_TBW => "TBW",
            EicCode::AREA_AMP => "AMP",
            EicCode::AREA_TTG => "TTG",
            EicCode::AREA_50HZ => "50Hz",
            EicCode::AREA_FR => "FR",
            EicCode::AREA_CH => "CH",
            EicCode::AREA_AT => "AT",
            EicCode::AREA_DK1A => "DK1A",
            EicCode::AREA_NO1A => "NO1A",
            EicCode::AREA_HR => "HR",
            EicCode::AREA_BG => "BG",
            EicCode::AREA_IE => "IE",
            EicCode::AREA_PL => "PL",
            EicCode::AREA_GB2 => "GB2",
            EicCode::AREA_ELE => "ELE",
            EicCode::AREA_ELI => "ELI",
            EicCode::AREA_ERI => "ERI",
            EicCode::AREA_GER => "GER",
            EicCode::AREA_LBE => "LBE",
            EicCode::AREA_LBI => "LBI",
            EicCode::AREA_LRE => "LRE",
            EicCode::AREA_LRI => "LRI",
            EicCode::AREA_PLA => "PLA",
            EicCode::AREA_ELES => "ELES",
            EicCode::AREA_CEPS => "CEPS",
            EicCode::AREA_MAVIR => "MAVIR",
            EicCode::AREA_TEL => "TEL",
            EicCode::AREA_GE => "GE",
            EicCode::AREA_GB1A => "GB1A",
            EicCode::AREA_PLC => "PLC",
            EicCode::AREA_ALBE => "ALBE",
            EicCode::AREA_ALDE => "ALDE",
            EicCode::AREA_ITCALA => "ITCALA",
            EicCode::AREA_ITFR => "ITFR",
            EicCode::AREA_ITCH => "ITCH",
            EicCode::AREA_ITAT => "ITAT",
            EicCode::AREA_ITSI => "ITSI",
            EicCode::AREA_ITME => "ITME",
            EicCode::AREA_ITGR => "ITGR",
            EicCode::AREA_ITCP => "ITCP",
            EicCode::AREA_DEA2 => "DEA2",
            EicCode::AREA_SK => "SK",
            EicCode::AREA_GREE => "GREE",
            EicCode::AREA_NO2A => "NO2A",
            EicCode::AREA_LKAL => "LKAL",
            EicCode::AREA_DE_LU => "DE-LU",
            EicCode::AREA_BY => "BY",
            EicCode::AREA_RU => "RU",
            EicCode::AREA_GB => "GB",
            EicCode::AREA_NO2_SK => "NO2_SK",
            EicCode::AREA_DK1_SK => "DK1_SK",
            EicCode::AREA_DK1_SB => "DK1_SB",
            EicCode::AREA_DK2_SB => "DK2_SB",
            EicCode::AREA_FI_FS => "FI_FS",
            EicCode::AREA_SE3_FS => "SE3_FS",
            EicCode::AREA_DK1_KS => "DK1_KS",
            EicCode::AREA_SE3_KS => "SE3_KS",
            EicCode::AREA_SE4_SP => "SE4_SP",
            EicCode::AREA_SE4_NB => "SE4_NB",
            EicCode::AREA_SE4_BC => "SE4_BC",
            EicCode::AREA_FI_EL => "FI_EL",
            EicCode::AREA_DK1_DE => "DK1_DE",
            EicCode::AREA_DK2_KO => "DK2_KO",
            EicCode::AREA_DK1_CO => "DK1_CO",
            EicCode::AREA_NO2_ND => "NO2_ND",
            EicCode::AREA_NO2_NK => "NO2_NK",
            EicCode::AREA_SE3_SWL => "SE3_SWL",
            EicCode::AREA_SE4_SWL => "SE4_SWL",
            EicCode::AREA_IS => "IS",
            EicCode::AREA_UA_IPS => "UA-IPS",
            EicCode::AREA_LU => "LU",
            EicCode::AREA_MD => "MD",
            EicCode::AREA_RS => "RS",
            EicCode::AREA_UA_BEI => "UA-BEI",
            EicCode::AREA_TR => "TR",
            EicCode::AREA_MK => "MK",
            EicCode::AREA_SE3A => "SE3A",
            EicCode::AREA_DE_DK1 => "DE_DK1",
            EicCode::AREA_DE_KO => "DE_KO",
            EicCode::AREA_DE_NK => "DE_NK",
            EicCode::AREA_DE_BC => "DE_BC",
            EicCode::AREA_NL_CO => "NL_CO",
            EicCode::AREA_NL_ND => "NL_ND",
            EicCode::AREA_PL_LP => "PL_LP",
            EicCode::AREA_PL_SP => "PL_SP",
            EicCode::AREA_RO_BG => "RO_BG",
        }
    }

    /// Look up an area by its PMD wire id.
    pub const fn from_id(id: DeliveryAreaId) -> Option<EicCode> {
        match id {
            1 => Some(EicCode::AREA_DK1),
            2 => Some(EicCode::AREA_DK2),
            3 => Some(EicCode::AREA_FI),
            4 => Some(EicCode::AREA_FRE),
            5 => Some(EicCode::AREA_EE),
            6 => Some(EicCode::AREA_LV),
            7 => Some(EicCode::AREA_LT),
            8 => Some(EicCode::AREA_NO1),
            9 => Some(EicCode::AREA_NO2),
            10 => Some(EicCode::AREA_NO3),
            11 => Some(EicCode::AREA_NO4),
            12 => Some(EicCode::AREA_NO5),
            13 => Some(EicCode::AREA_SE1),
            14 => Some(EicCode::AREA_SE2),
            15 => Some(EicCode::AREA_SE3),
            16 => Some(EicCode::AREA_SE4),
            18 => Some(EicCode::AREA_CNOR),
            19 => Some(EicCode::AREA_COAC),
            20 => Some(EicCode::AREA_CORS),
            21 => Some(EicCode::AREA_CSUD),
            23 => Some(EicCode::AREA_MALTA),
            24 => Some(EicCode::AREA_NORD),
            27 => Some(EicCode::AREA_SARD),
            28 => Some(EicCode::AREA_SICI),
            29 => Some(EicCode::AREA_SUD),
            30 => Some(EicCode::AREA_PT),
            31 => Some(EicCode::AREA_ES),
            32 => Some(EicCode::AREA_MO),
            33 => Some(EicCode::AREA_UK),
            34 => Some(EicCode::AREA_NL),
            35 => Some(EicCode::AREA_BE),
            36 => Some(EicCode::AREA_TBW),
            37 => Some(EicCode::AREA_AMP),
            38 => Some(EicCode::AREA_TTG),
            39 => Some(EicCode::AREA_50HZ),
            40 => Some(EicCode::AREA_FR),
            41 => Some(EicCode::AREA_CH),
            42 => Some(EicCode::AREA_AT),
            43 => Some(EicCode::AREA_DK1A),
            44 => Some(EicCode::AREA_NO1A),
            45 => Some(EicCode::AREA_HR),
            46 => Some(EicCode::AREA_BG),
            86 => Some(EicCode::AREA_IE),
            88 => Some(EicCode::AREA_PL),
            90 => Some(EicCode::AREA_GB2),
            92 => Some(EicCode::AREA_ELE),
            93 => Some(EicCode::AREA_ELI),
            94 => Some(EicCode::AREA_ERI),
            95 => Some(EicCode::AREA_GER),
            96 => Some(EicCode::AREA_LBE),
            97 => Some(EicCode::AREA_LBI),
            98 => Some(EicCode::AREA_LRE),
            99 => Some(EicCode::AREA_LRI),
            100 => Some(EicCode::AREA_PLA),
            108 => Some(EicCode::AREA_ELES),
            109 => Some(EicCode::AREA_CEPS),
            110 => Some(EicCode::AREA_MAVIR),
            111 => Some(EicCode::AREA_TEL),
            113 => Some(EicCode::AREA_GE),
            114 => Some(EicCode::AREA_GB1A),
            115 => Some(EicCode::AREA_PLC),
            116 => Some(EicCode::AREA_ALBE),
            117 => Some(EicCode::AREA_ALDE),
            118 => Some(EicCode::AREA_ITCALA),
            120 => Some(EicCode::AREA_ITFR),
            122 => Some(EicCode::AREA_ITCH),
            124 => Some(EicCode::AREA_ITAT),
            126 => Some(EicCode::AREA_ITSI),
            128 => Some(EicCode::AREA_ITME),
            130 => Some(EicCode::AREA_ITGR),
            132 => Some(EicCode::AREA_ITCP),
            134 => Some(EicCode::AREA_DEA2),
            135 => Some(EicCode::AREA_SK),
            136 => Some(EicCode::AREA_GREE),
            137 => Some(EicCode::AREA_NO2A),
            138 => Some(EicCode::AREA_LKAL),
            139 => Some(EicCode::AREA_DE_LU),
            140 => Some(EicCode::AREA_BY),
            141 => Some(EicCode::AREA_RU),
            142 => Some(EicCode::AREA_GB),
            143 => Some(EicCode::AREA_NO2_SK),
            144 => Some(EicCode::AREA_DK1_SK),
            145 => Some(EicCode::AREA_DK1_SB),
            146 => Some(EicCode::AREA_DK2_SB),
            147 => Some(EicCode::AREA_FI_FS),
            148 => Some(EicCode::AREA_SE3_FS),
            149 => Some(EicCode::AREA_DK1_KS),
            150 => Some(EicCode::AREA_SE3_KS),
            151 => Some(EicCode::AREA_SE4_SP),
            152 => Some(EicCode::AREA_SE4_NB),
            153 => Some(EicCode::AREA_SE4_BC),
            154 => Some(EicCode::AREA_FI_EL),
            155 => Some(EicCode::AREA_DK1_DE),
            156 => Some(EicCode::AREA_DK2_KO),
            157 => Some(EicCode::AREA_DK1_CO),
            158 => Some(EicCode::AREA_NO2_ND),
            159 => Some(EicCode::AREA_NO2_NK),
            160 => Some(EicCode::AREA_SE3_SWL),
            161 => Some(EicCode::AREA_SE4_SWL),
            162 => Some(EicCode::AREA_IS),
            172 => Some(EicCode::AREA_UA_IPS),
            173 => Some(EicCode::AREA_LU),
            174 => Some(EicCode::AREA_MD),
            175 => Some(EicCode::AREA_RS),
            177 => Some(EicCode::AREA_UA_BEI),
            178 => Some(EicCode::AREA_TR),
            179 => Some(EicCode::AREA_MK),
            180 => Some(EicCode::AREA_SE3A),
            181 => Some(EicCode::AREA_DE_DK1),
            182 => Some(EicCode::AREA_DE_KO),
            183 => Some(EicCode::AREA_DE_NK),
            184 => Some(EicCode::AREA_DE_BC),
            185 => Some(EicCode::AREA_NL_CO),
            186 => Some(EicCode::AREA_NL_ND),
            187 => Some(EicCode::AREA_PL_LP),
            188 => Some(EicCode::AREA_PL_SP),
            189 => Some(EicCode::AREA_RO_BG),
            _ => None,
        }
    }

    /// Look up an area by its EIC code string.
    pub fn from_eic(eic: &str) -> Option<EicCode> {
        match eic {
            "10YDK-1--------W" => Some(EicCode::AREA_DK1),
            "10YDK-2--------M" => Some(EicCode::AREA_DK2),
            "10YFI-1--------U" => Some(EicCode::AREA_FI),
            "10YDOM-1001A084H" => Some(EicCode::AREA_FRE),
            "10Y1001A1001A39I" => Some(EicCode::AREA_EE),
            "10YLV-1001A00074" => Some(EicCode::AREA_LV),
            "10YLT-1001A0008Q" => Some(EicCode::AREA_LT),
            "10YNO-1--------2" => Some(EicCode::AREA_NO1),
            "10YNO-2--------T" => Some(EicCode::AREA_NO2),
            "10YNO-3--------J" => Some(EicCode::AREA_NO3),
            "10YNO-4--------9" => Some(EicCode::AREA_NO4),
            "10Y1001A1001A48H" => Some(EicCode::AREA_NO5),
            "10Y1001A1001A44P" => Some(EicCode::AREA_SE1),
            "10Y1001A1001A45N" => Some(EicCode::AREA_SE2),
            "10Y1001A1001A46L" => Some(EicCode::AREA_SE3),
            "10Y1001A1001A47J" => Some(EicCode::AREA_SE4),
            "10Y1001A1001A70O" => Some(EicCode::AREA_CNOR),
            "10Y1001A1001A885" => Some(EicCode::AREA_COAC),
            "10Y1001A1001A893" => Some(EicCode::AREA_CORS),
            "10Y1001A1001A71M" => Some(EicCode::AREA_CSUD),
            "10Y1001A1001A877" => Some(EicCode::AREA_MALTA),
            "10Y1001A1001A73I" => Some(EicCode::AREA_NORD),
            "10Y1001A1001A74G" => Some(EicCode::AREA_SARD),
            "10Y1001A1001A75E" => Some(EicCode::AREA_SICI),
            "10Y1001A1001A788" => Some(EicCode::AREA_SUD),
            "10YPT-REN------W" => Some(EicCode::AREA_PT),
            "10YES-REE------0" => Some(EicCode::AREA_ES),
            "10YMA-ONE------O" => Some(EicCode::AREA_MO),
            "10Y1001A1001A57G" => Some(EicCode::AREA_UK),
            "10YNL----------L" => Some(EicCode::AREA_NL),
            "10YBE----------2" => Some(EicCode::AREA_BE),
            "10YDE-ENBW-----N" => Some(EicCode::AREA_TBW),
            "10YDE-RWENET---I" => Some(EicCode::AREA_AMP),
            "10YDE-EON------1" => Some(EicCode::AREA_TTG),
            "10YDE-VE-------2" => Some(EicCode::AREA_50HZ),
            "10YFR-RTE------C" => Some(EicCode::AREA_FR),
            "10YCH-SWISSGRIDZ" => Some(EicCode::AREA_CH),
            "10YAT-APG------L" => Some(EicCode::AREA_AT),
            "10YDK-1-------AA" => Some(EicCode::AREA_DK1A),
            "10Y1001A1001A64J" => Some(EicCode::AREA_NO1A),
            "10YHR-HEP------M" => Some(EicCode::AREA_HR),
            "10YCA-BULGARIA-R" => Some(EicCode::AREA_BG),
            "10Y1001A1001A59C" => Some(EicCode::AREA_IE),
            "10YPL-AREA-----S" => Some(EicCode::AREA_PL),
            "10Y1001A1001A58E" => Some(EicCode::AREA_GB2),
            "38YNPSLVEXP----Y" => Some(EicCode::AREA_ELE),
            "38YNPSLVIMP----5" => Some(EicCode::AREA_ELI),
            "38YNPSRUIMP----S" => Some(EicCode::AREA_ERI),
            "10YCB-GERMANY--8" => Some(EicCode::AREA_GER),
            "10Y1001A1001A56I" => Some(EicCode::AREA_LBE),
            "10Y1001A1001A55K" => Some(EicCode::AREA_LBI),
            "43YLRE-------008" => Some(EicCode::AREA_LRE),
            "43YLRI--------04" => Some(EicCode::AREA_LRI),
            "10YDOM-PL-SE-LT2" => Some(EicCode::AREA_PLA),
            "10YSI-ELES-----O" => Some(EicCode::AREA_ELES),
            "10YCZ-CEPS-----N" => Some(EicCode::AREA_CEPS),
            "10YHU-MAVIR----U" => Some(EicCode::AREA_MAVIR),
            "10YRO-TEL------P" => Some(EicCode::AREA_TEL),
            "10Y1001A1001B012" => Some(EicCode::AREA_GE),
            "11Y0-0000-0265-K" => Some(EicCode::AREA_GB1A),
            "10Y1001C--00038X" => Some(EicCode::AREA_PLC),
            "22Y201903144---9" => Some(EicCode::AREA_ALBE),
            "22Y201903145---4" => Some(EicCode::AREA_ALDE),
            "10Y1001C--00096J" => Some(EicCode::AREA_ITCALA),
            "10Y1001A1001A81J" => Some(EicCode::AREA_ITFR),
            "10Y1001A1001A68B" => Some(EicCode::AREA_ITCH),
            "10Y1001A1001A80L" => Some(EicCode::AREA_ITAT),
            "10Y1001A1001A67D" => Some(EicCode::AREA_ITSI),
            "10Y1001C--000611" => Some(EicCode::AREA_ITME),
            "10Y1001A1001A66F" => Some(EicCode::AREA_ITGR),
            "10YIT-GRTN-----B" => Some(EicCode::AREA_ITCP),
            "10YDOM-1001A082L" => Some(EicCode::AREA_DEA2),
            "10YSK-SEPS-----K" => Some(EicCode::AREA_SK),
            "10YGR-HTSO-----Y" => Some(EicCode::AREA_GREE),
            "10Y1001C--001219" => Some(EicCode::AREA_NO2A),
            "10Y1001A1001A50U" => Some(EicCode::AREA_LKAL),
            "10Y1001A1001A82H" => Some(EicCode::AREA_DE_LU),
            "10Y1001A1001A51S" => Some(EicCode::AREA_BY),
            "10Y1001A1001A49F" => Some(EicCode::AREA_RU),
            "10YGB----------A" => Some(EicCode::AREA_GB),
            "50YCUY85S1HH29EK" => Some(EicCode::AREA_NO2_SK),
            "45Y000000000001C" => Some(EicCode::AREA_DK1_SK),
            "45Y0000000000038" => Some(EicCode::AREA_DK1_SB),
            "45Y0000000000062" => Some(EicCode::AREA_DK2_SB),
            "44Y-00000000160K" => Some(EicCode::AREA_FI_FS),
            "46Y000000000001Y" => Some(EicCode::AREA_SE3_FS),
            "45Y000000000002A" => Some(EicCode::AREA_DK1_KS),
            "46Y000000000002W" => Some(EicCode::AREA_SE3_KS),
            "46Y000000000003U" => Some(EicCode::AREA_SE4_SP),
            "46Y000000000004S" => Some(EicCode::AREA_SE4_NB),
            "46Y000000000005Q" => Some(EicCode::AREA_SE4_BC),
            "44Y-00000000161I" => Some(EicCode::AREA_FI_EL),
            "45Y0000000000054" => Some(EicCode::AREA_DK1_DE),
            "45Y0000000000070" => Some(EicCode::AREA_DK2_KO),
            "45Y0000000000046" => Some(EicCode::AREA_DK1_CO),
            "50Y73EMZ34CQL9AJ" => Some(EicCode::AREA_NO2_ND),
            "50YNBFFTWZRAHA3P" => Some(EicCode::AREA_NO2_NK),
            "46Y000000000017J" => Some(EicCode::AREA_SE3_SWL),
            "46Y000000000018H" => Some(EicCode::AREA_SE4_SWL),
            "10Y1001A1001A958" => Some(EicCode::AREA_IS),
            "10Y1001C--000182" => Some(EicCode::AREA_UA_IPS),
            "10YLU-CEGEDEL-NQ" => Some(EicCode::AREA_LU),
            "10Y1001A1001A990" => Some(EicCode::AREA_MD),
            "10YCS-SERBIATSOV" => Some(EicCode::AREA_RS),
            "10YUA-WEPS-----0" => Some(EicCode::AREA_UA_BEI),
            "10YTR-TEIAS----W" => Some(EicCode::AREA_TR),
            "10YMK-MEPSO----8" => Some(EicCode::AREA_MK),
            "10Y1001C--00148Q" => Some(EicCode::AREA_SE3A),
            "10YDK1-DE-10010E" => Some(EicCode::AREA_DE_DK1),
            "10Y1001C--001510" => Some(EicCode::AREA_DE_KO),
            "10Y1001C--00152Z" => Some(EicCode::AREA_DE_NK),
            "10YDE-SE4-VBZ-01" => Some(EicCode::AREA_DE_BC),
            "49Y000000000003M" => Some(EicCode::AREA_NL_CO),
            "49Y000000000004K" => Some(EicCode::AREA_NL_ND),
            "10Y1001C--00153X" => Some(EicCode::AREA_PL_LP),
            "10Y1001C--00154V" => Some(EicCode::AREA_PL_SP),
            "10Y1001C--00158N" => Some(EicCode::AREA_RO_BG),
            _ => None,
        }
    }
}

impl std::fmt::Display for EicCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.eic_code())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::EicCode;

    /// The bundled reference CSV this enum is the compiled form of.
    const REFERENCE_CSV: &str = include_str!("../delivery_areas.csv");

    #[test]
    fn enum_matches_the_reference_csv_exactly() {
        // The CSV is the data of record (refreshed from the archive by depth-replay); this
        // pins the hardcoded enum to it variant-for-variant, replacing the old build-time
        // codegen's sync guarantee. Both are ordered by wire id.
        let rows: Vec<(i32, &str, &str)> = REFERENCE_CSV
            .lines()
            .skip(1)
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let mut fields = line.split(',');
                let id = fields.next().unwrap().parse().unwrap();
                let area_code = fields.next().unwrap();
                let eic = fields.next().unwrap();
                (id, area_code, eic)
            })
            .collect();
        assert_eq!(rows.len(), EicCode::ALL.len(), "row/variant count mismatch");
        for (&area, &(id, area_code, eic)) in EicCode::ALL.iter().zip(&rows) {
            assert_eq!(area.id(), id, "{area:?}");
            assert_eq!(area.area_code(), area_code, "{area:?}");
            assert_eq!(area.eic_code(), eic, "{area:?}");
        }
    }

    #[test]
    fn known_area_maps_between_id_eic_and_area_code() {
        let fi = EicCode::from_eic("10YFI-1--------U").unwrap();
        assert_eq!(fi, EicCode::AREA_FI);
        assert_eq!(fi.id(), 3);
        assert_eq!(fi.area_code(), "FI");
        assert_eq!(fi.to_string(), "10YFI-1--------U");
        assert_eq!(EicCode::from_id(3), Some(fi));
    }

    #[test]
    fn unknown_id_and_eic_resolve_to_none() {
        assert_eq!(EicCode::from_id(0), None);
        assert_eq!(EicCode::from_id(-1), None);
        assert_eq!(EicCode::from_eic("10YNO-SUCH-AREA-"), None);
        assert_eq!(EicCode::from_eic(""), None);
    }

    #[test]
    fn all_areas_roundtrip_and_are_unique() {
        // The generated table must be a bijection in both directions — the config resolves
        // EIC → id and the sinks resolve id → EIC, so a duplicate on either side would
        // silently misroute an area.
        let mut ids = HashSet::new();
        let mut eics = HashSet::new();
        for &area in EicCode::ALL {
            assert!(ids.insert(area.id()), "duplicate id {}", area.id());
            assert!(
                eics.insert(area.eic_code()),
                "duplicate eic {}",
                area.eic_code()
            );
            assert_eq!(EicCode::from_id(area.id()), Some(area));
            assert_eq!(EicCode::from_eic(area.eic_code()), Some(area));
        }
    }
}
