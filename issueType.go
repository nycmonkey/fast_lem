package fast_lem

type IssueType int

const (
	NA IssueType = iota
	AB
	AD
	AG
	AI
	BC
	BD
	CA
	CE
	CP
	DB
	DL
	DR
	EP
	EQ
	ET
	FM
	FU
	FX
	ID
	LN
	MB
	MT
	MU
	NT
	OE
	OP
	PF
	PQ
	PV
	SH
	UI
	UL
	US
	WT
)

func IssueTypeFromString(code string) IssueType {
	switch code {
	case "AB":
		return AB
	case "AD":
		return AD
	case "AG":
		return AG
	case "AI":
		return AI
	case "BC":
		return BC
	case "BD":
		return BD
	case "CA":
		return CA
	case "CE":
		return CE
	case "CP":
		return CP
	case "DB":
		return DB
	case "DL":
		return DL
	case "DR":
		return DR
	case "EP":
		return EP
	case "EQ":
		return EQ
	case "ET":
		return ET
	case "FM":
		return FM
	case "FU":
		return FU
	case "FX":
		return FX
	case "ID":
		return ID
	case "LN":
		return LN
	case "MB":
		return MB
	case "MT":
		return MT
	case "MU":
		return MU
	case "NT":
		return NT
	case "OE":
		return OE
	case "OP":
		return OP
	case "PF":
		return PF
	case "PQ":
		return PQ
	case "PV":
		return PV
	case "SH":
		return SH
	case "UI":
		return UI
	case "UL":
		return UL
	case "US":
		return US
	case "WT":
		return WT
	default:
		return NA
	}
}

func (it IssueType) String() string {
	switch it {
	case NA:
		return "N/A"
	case AB:
		return "Asset-Backed"
	case AD:
		return "ADR/GDR"
	case AG:
		return "Agency Bond"
	case AI:
		return "Alternative Invt"
	case BC:
		return "Convertible Bond"
	case BD:
		return "Bond"
	case CA:
		return "Cash/Repo/MM"
	case CE:
		return "Closed-End Mutual Fund"
	case CP:
		return "Convertible Preferred"
	case DB:
		return "Debenture"
	case DL:
		return "Dual Listing"
	case DR:
		return "Derivative"
	case EP:
		return "Equity (Pre-IPO)"
	case EQ:
		return "Equity"
	case ET:
		return "Exchange Traded Fund"
	case FM:
		return "First Mortgage"
	case FU:
		return "Future Agreement"
	case FX:
		return "Fixed Income/Unclassified"
	case ID:
		return "Index"
	case LN:
		return "Loan"
	case MB:
		return "Mortgage-Backed"
	case MT:
		return "Medium Term Note"
	case MU:
		return "Municipal Bonds"
	case NT:
		return "Note"
	case OE:
		return "Open-End Mutual Fund"
	case OP:
		return "Stock Option"
	case PF:
		return "Preferred"
	case PQ:
		return "Private Equity"
	case PV:
		return "Private Placement"
	case SH:
		return "Short Position"
	case UI:
		return "Unit Invt Trust"
	case UL:
		return "Treasury/Long-Term"
	case US:
		return "Treasury/Short-Term"
	case WT:
		return "Warrant/Right"
	default:
		return "Unknown Issue Type"
	}
}
