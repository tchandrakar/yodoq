package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"yodoq/observer"
	"yodoq/solution"
)

const maxEvents = 500

var (
	headerStyle  = lipgloss.NewStyle().Bold(true)
	statusOpen   = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	statusClosed = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	reclaimStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	errStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
)

type model struct {
	addr      string
	connected bool
	events    []observer.Event // newest first, bounded to maxEvents
	lastStats *solution.Stats
	msgCh     chan any
	ready     bool
	vp        viewport.Model
}

func newModel(addr string) model {
	return model{addr: addr, msgCh: make(chan any, 256)}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(connectCmd(m.addr), waitForMsg(m.msgCh))
}

// connectCmd dials addr and kicks off a background read loop feeding
// m.msgCh; on dial failure it schedules a retry after reconnectDelay.
func connectCmd(addr string) tea.Cmd {
	return func() tea.Msg {
		conn, err := dial(addr)
		if err != nil {
			time.Sleep(reconnectDelay)
			return disconnectedMsg{err: err}
		}
		return connectedMsg{conn: conn}
	}
}

// waitForMsg blocks on the shared channel for the next message from the
// read-loop goroutine, feeding it back into bubbletea's Update loop.
func waitForMsg(ch chan any) tea.Cmd {
	return func() tea.Msg {
		return <-ch
	}
}

func (m model) headerHeight() int { return 3 }
func (m model) footerHeight() int { return 2 }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		vpHeight := msg.Height - m.headerHeight() - m.footerHeight()
		if vpHeight < 1 {
			vpHeight = 1
		}
		if !m.ready {
			m.vp = viewport.New(msg.Width, vpHeight)
			m.ready = true
		} else {
			m.vp.Width = msg.Width
			m.vp.Height = vpHeight
		}
		m.vp.SetContent(m.renderEvents())
		return m, nil
	case connectedMsg:
		m.connected = true
		go readLoop(msg.conn, m.msgCh)
		return m, waitForMsg(m.msgCh)
	case disconnectedMsg:
		m.connected = false
		return m, tea.Batch(connectCmd(m.addr), waitForMsg(m.msgCh))
	case envelopeMsg:
		m.events = append([]observer.Event{msg.envelope.Event}, m.events...)
		if len(m.events) > maxEvents {
			m.events = m.events[:maxEvents]
		}
		if msg.envelope.Event.Kind == observer.KindStatsSnapshot {
			m.lastStats = msg.envelope.Event.Stats
		}
		if m.ready {
			atTop := m.vp.AtTop()
			m.vp.SetContent(m.renderEvents())
			if atTop {
				m.vp.GotoTop()
			}
		}
		return m, waitForMsg(m.msgCh)
	}

	var cmd tea.Cmd
	m.vp, cmd = m.vp.Update(msg)
	return m, cmd
}

func (m model) renderEvents() string {
	lines := make([]string, len(m.events))
	for i, e := range m.events {
		lines[i] = renderEvent(e)
	}
	return strings.Join(lines, "\n")
}

func (m model) View() string {
	if !m.ready {
		return "initializing…"
	}

	status := statusClosed.Render("disconnected")
	if m.connected {
		status = statusOpen.Render("connected")
	}
	header := fmt.Sprintf("%s  %s  %s", headerStyle.Render("YODOQ TUI"), m.addr, status)
	if m.lastStats != nil {
		header += fmt.Sprintf("\nPending=%d Leased=%d Completed=%d Abandoned=%d",
			m.lastStats.Pending, m.lastStats.Leased, m.lastStats.Completed, m.lastStats.Abandoned)
	} else {
		header += "\n"
	}

	return header + "\n" + m.vp.View() + "\n(↑/↓ to scroll, q to quit)"
}

func renderEvent(e observer.Event) string {
	line := fmt.Sprintf("#%d %-18s", e.Seq, e.Kind)
	if e.JobID != "" {
		line += " job=" + e.JobID
	}
	if e.WorkerID != "" {
		line += " worker=" + e.WorkerID
	}
	if e.PrevWorkerID != "" {
		line += " prevWorker=" + e.PrevWorkerID
	}
	if e.ScenarioName != "" {
		line += " scenario=" + e.ScenarioName
	}
	if e.Err != "" {
		return errStyle.Render(line + " err=" + e.Err)
	}
	if e.Kind == observer.KindLeaseReclaimed {
		return reclaimStyle.Render(line)
	}
	return line
}
