// Command tui is a terminal dashboard for the YODOQ observability event
// stream. It's a WebSocket client of the same protocol the browser
// dashboard (web/) speaks, and — like the browser — doesn't care whether
// it's pointed at cmd/playground or cmd/harness-replay.
package main

import (
	"flag"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	addr := flag.String("addr", "ws://localhost:8080/ws", "WebSocket address to connect to (playground default :8080, harness-replay default :8081)")
	flag.Parse()

	p := tea.NewProgram(newModel(*addr), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "tui:", err)
		os.Exit(1)
	}
}
