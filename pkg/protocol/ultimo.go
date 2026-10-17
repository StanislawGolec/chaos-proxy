package protocol

import (
	"errors"
	"fmt"
	"strings"
)

// CardColor represents standard Ultimo game colors.
type CardColor string

const (
	ColorRed    CardColor = "RED"
	ColorBlue   CardColor = "BLUE"
	ColorGreen  CardColor = "GREEN"
	ColorYellow CardColor = "YELLOW"
	ColorWild   CardColor = "WILD"
)

// CardValue represents standard Ultimo card face values.
type CardValue string

const (
	ValZero         CardValue = "ZERO"
	ValOne          CardValue = "ONE"
	ValTwo          CardValue = "TWO"
	ValThree        CardValue = "THREE"
	ValFour         CardValue = "FOUR"
	ValFive         CardValue = "FIVE"
	ValSix          CardValue = "SIX"
	ValSeven        CardValue = "SEVEN"
	ValEight        CardValue = "EIGHT"
	ValNine         CardValue = "NINE"
	ValSkip         CardValue = "SKIP"
	ValReverse      CardValue = "REVERSE"
	ValDrawTwo      CardValue = "DRAW_TWO"
	ValWild         CardValue = "WILD"
	ValWildDrawFour CardValue = "WILD_DRAW_FOUR"
)

// Card encapsulates a validated game card token.
type Card struct {
	Color CardColor
	Value CardValue
}

// ParseCard decodes a wire representation token into a strongly typed Card.
// Compliant with Inspiration/Ultimo-Protocol/ProtocolCodec.java.
func ParseCard(token string) (Card, error) {
	norm := strings.ToUpper(strings.TrimSpace(token))
	if norm == "" {
		return Card{}, errors.New("empty card token")
	}

	if norm == "WILD" || norm == "WILD_WILD" {
		return Card{Color: ColorWild, Value: ValWild}, nil
	}
	if norm == "WILD_DRAW_FOUR" || norm == "WILD_DRAW4" || norm == "WILD_WILD_DRAW_FOUR" {
		return Card{Color: ColorWild, Value: ValWildDrawFour}, nil
	}

	idx := strings.Index(norm, "_")
	if idx <= 0 || idx >= len(norm)-1 {
		return Card{}, fmt.Errorf("invalid card format, expected COLOR_VALUE: %s", token)
	}

	colorStr := norm[:idx]
	valueStr := norm[idx+1:]

	var color CardColor
	switch colorStr {
	case "RED":
		color = ColorRed
	case "BLUE":
		color = ColorBlue
	case "GREEN":
		color = ColorGreen
	case "YELLOW":
		color = ColorYellow
	default:
		return Card{}, fmt.Errorf("unknown card color: %s", colorStr)
	}

	var value CardValue
	switch valueStr {
	case "0", "ZERO":
		value = ValZero
	case "1", "ONE":
		value = ValOne
	case "2", "TWO":
		value = ValTwo
	case "3", "THREE":
		value = ValThree
	case "4", "FOUR":
		value = ValFour
	case "5", "FIVE":
		value = ValFive
	case "6", "SIX":
		value = ValSix
	case "7", "SEVEN":
		value = ValSeven
	case "8", "EIGHT":
		value = ValEight
	case "9", "NINE":
		value = ValNine
	case "SKIP":
		value = ValSkip
	case "REVERSE":
		value = ValReverse
	case "DRAW_TWO", "DRAW2", "DRAW_2":
		value = ValDrawTwo
	case "WILD":
		value = ValWild
	case "WILD_DRAW_FOUR", "WILD_DRAW4":
		value = ValWildDrawFour
	default:
		return Card{}, fmt.Errorf("unknown card value: %s", valueStr)
	}

	return Card{Color: color, Value: value}, nil
}

// FormatCard serializes a Card back into wire representation token.
func FormatCard(c Card) string {
	if c.Value == ValWild {
		return "WILD"
	}
	if c.Value == ValWildDrawFour {
		return "WILD_DRAW_FOUR"
	}
	return fmt.Sprintf("%s_%s", c.Color, c.Value)
}

// MessageCategory distinguishes commands originating from clients vs events from the game server.
type MessageCategory string

const (
	CategoryCommand MessageCategory = "CLIENT_COMMAND"
	CategoryEvent   MessageCategory = "SERVER_EVENT"
	CategoryUnknown MessageCategory = "UNKNOWN"
)

// UltimoMessage represents a parsed line-delimited message.
type UltimoMessage struct {
	Category MessageCategory
	Verb     string
	Args     []string
	Raw      string
}

// ParseUltimoMessage decodes an ASCII wire line (with or without trailing newline) into an UltimoMessage.
func ParseUltimoMessage(line string) (*UltimoMessage, error) {
	trimmed := strings.TrimRight(line, "\r\n")
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return nil, errors.New("empty protocol line")
	}

	parts := strings.Fields(trimmed)
	if len(parts) == 0 {
		return nil, errors.New("empty protocol tokens")
	}

	verb := strings.ToUpper(parts[0])
	args := parts[1:]

	category := CategoryUnknown
	switch UltimoCommand(verb) {
	case CmdJoin, CmdStart, CmdPlay, CmdDraw, CmdPass, CmdPing, CmdChat:
		category = CategoryCommand
	}

	if category == CategoryUnknown {
		switch UltimoEvent(verb) {
		case EventWelcome, EventRoomStatus, EventHandUpdate, EventTopCard,
			EventTurnAnnounced, EventOpponentUpdate, EventDrawResult,
			EventGameOver, EventError, EventPong:
			category = CategoryEvent
		}
	}

	return &UltimoMessage{
		Category: category,
		Verb:     verb,
		Args:     args,
		Raw:      trimmed,
	}, nil
}
