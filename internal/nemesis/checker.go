package nemesis

import (
	"fmt"
	"math"
	"time"

	"github.com/anishathalye/porcupine"
)

// Linearizability checking (M6 part 2): the recorded history is checked
// against a sequential register-per-key KV model with Porcupine.
//
// Model semantics and the honest treatment of uncertainty:
//
//   - Each key is an independent register (puts overwrite, delete empties,
//     get observes). Keys partition the history — linearizability composes
//     over independent objects, and per-key checking turns an exponential
//     search into many small ones.
//
//   - An ACKED op executed exactly once, somewhere in [invoke, return].
//
//   - An UNKNOWN put (every attempt timed out) may or may not ever take
//     effect, and if it does, at any time after invoke. Standard Jepsen
//     treatment: keep it with an infinite window. The checker must then
//     linearize it SOMEWHERE — if it never actually executed, placing it
//     after every observation is always legal, so phantom placement never
//     causes a false alarm; if it DID execute, the checker has the window
//     to prove it. UNKNOWN gets and deletes constrain nothing (no output
//     was observed, deletes are idempotent) and are dropped.
//
//   - Session dedup gives the system a property the register model does
//     not encode: an unknown OLDER-serial write can never apply after a
//     newer acked one. The model is therefore slightly MORE permissive
//     than the system — sound (no false alarms), marginally less strict.
//     Encoding serials into the model would tighten it; documented as
//     future work, not silently skipped.

// kvInput / kvOutput are the model's op encoding.
type kvInput struct {
	Kind  string // "put" | "get" | "delete"
	Key   string
	Value string
}

type kvOutput struct {
	Found bool
	Value string
}

// kvModel: state is the register's value; "" + absent is modeled as
// !found. State is a *string-ish* pair encoded as kvOutput for reuse.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).Key
			byKey[k] = append(byKey[k], op)
		}
		parts := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			parts = append(parts, ops)
		}
		return parts
	},
	Init: func() interface{} { return kvOutput{} }, // absent
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(kvOutput)
		in := input.(kvInput)
		switch in.Kind {
		case "put":
			return true, kvOutput{Found: true, Value: in.Value}
		case "delete":
			return true, kvOutput{}
		case "get":
			out := output.(kvOutput)
			return out.Found == st.Found && (!st.Found || out.Value == st.Value), st
		}
		return false, st
	},
	Equal: func(a, b interface{}) bool { return a == b },
	DescribeOperation: func(input, output interface{}) string {
		in := input.(kvInput)
		switch in.Kind {
		case "get":
			out := output.(kvOutput)
			if !out.Found {
				return fmt.Sprintf("get(%s) -> absent", in.Key)
			}
			return fmt.Sprintf("get(%s) -> %s", in.Key, out.Value)
		case "put":
			return fmt.Sprintf("put(%s, %s)", in.Key, in.Value)
		default:
			return fmt.Sprintf("delete(%s)", in.Key)
		}
	},
}

// CheckLinearizability converts a recorded history and runs Porcupine.
// ok=false means a real linearizability violation was found (the returned
// info can render a visualization); err covers checker timeout.
func CheckLinearizability(ops []Op, timeout time.Duration) (bool, error) {
	history := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		if op.Unknown && op.Kind != "put" {
			continue // an unobserved read/delete constrains nothing
		}
		// Timestamps arrive tie-free: the Recorder's clock is strictly
		// monotonic (WS-2 — ties from coarse clocks made the checker
		// blind, and the first attempted fix, an interval transform,
		// inverted sub-tick ops' windows and manufactured violations).
		pop := porcupine.Operation{
			ClientId: op.Client,
			Call:     op.Invoke,
			Return:   op.Return,
		}
		switch op.Kind {
		case "put":
			pop.Input = kvInput{Kind: "put", Key: op.Key, Value: op.Value}
			pop.Output = kvOutput{}
			if op.Unknown {
				pop.Return = math.MaxInt64 // may take effect at any later time
			}
		case "get":
			pop.Input = kvInput{Kind: "get", Key: op.Key}
			pop.Output = kvOutput{Found: op.Found, Value: op.Output}
		case "delete":
			pop.Input = kvInput{Kind: "delete", Key: op.Key}
			pop.Output = kvOutput{}
		default:
			return false, fmt.Errorf("history contains unknown op kind %q", op.Kind)
		}
		history = append(history, pop)
	}

	res, _ := porcupine.CheckOperationsVerbose(kvModel, history, timeout)
	switch res {
	case porcupine.Ok:
		return true, nil
	case porcupine.Illegal:
		return false, nil
	default:
		return false, fmt.Errorf("linearizability check timed out after %s (%d ops)", timeout, len(history))
	}
}

// FindViolatingKey narrows an Illegal verdict to evidence: keys are
// independent registers, so at least one key's sub-history must fail on
// its own. Returns that key and its ops (empty if, unexpectedly, every
// per-key check passes — which would indicate a checker bug, since the
// full check partitions by key too).
func FindViolatingKey(ops []Op, timeout time.Duration) (string, []Op) {
	byKey := map[string][]Op{}
	for _, op := range ops {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	for key, kops := range byKey {
		if ok, err := CheckLinearizability(kops, timeout); err == nil && !ok {
			return key, kops
		}
	}
	return "", nil
}
