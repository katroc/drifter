# Drifter UX Design Critique & Roadmap

## 1. Information Architecture & Mental Models

**The "Hopper" vs. "Remote" Disconnect**
*   **The Flaw:** Broken mental model of file transfer. Users expect a "Source" and a "Destination" visually linked.
*   **The Friction:** Uploading in Tab 1 and checking results in Tab 2 requires excessive context switching.
*   **The Fix:** Adopt the **"Commander" pattern**. Side-by-side panels: Left = Local, Right = Remote. Center/Bottom = Queue.

**Jargon Overload**
*   **The Flaw:** "Hopper" is a clever name but adds cognitive load for new users.
*   **The Friction:** Users have to learn a custom dictionary to navigate basic file operations.
*   **The Fix:** Rename "Hopper" to **"Local"** or **"Files"**. Clarity > Cleverness.

---

## 2. Screen Real Estate & Layout

**The "Boxy" Fatigue**
*   **The Flaw:** Every component is wrapped in heavy borders (Rail, Hopper, Queue, History, Footer).
*   **The Friction:** High visual noise. The eye gets stuck on grid lines rather than content, creating a claustrophobic feel.
*   **The Fix:** Use whitespace or background dimming to separate areas. Remove redundant inner borders.

---

## 3. Interaction Design (IxD)

**Invisible Affordances**
*   **The Flaw:** Keybindings (Space to select, 's' to queue) rely entirely on user memory.
*   **The Friction:** Intermittent users will struggle to remember commands without constant visual cues.
*   **The Fix:** **Persistent Keybinding Bar**. Add 1-2 rows at the bottom that dynamically show context-relevant keys (e.g., `^Q Quit`, `^S Upload`).

**Focus State Ambiguity**
*   **The Flaw:** Relying solely on border color to show focus is too subtle.
*   **The Friction:** Difficult to quickly identify the active panel at a glance.
*   **The Fix:** **Dim Inactive Panels**. Active panels should be bright/bold; inactive panels should gray out or dim their text.

**Mouse Interaction "Uncanny Valley"**
*   **The Flaw:** TUI elements don't react until clicked (no hover/pre-light states).
*   **The Friction:** Interactions feel disconnected compared to modern GUI expectations.
*   **The Fix:** Implement row highlighting on mouse-over (terminal-dependent) to make the app feel responsive.

---

## 4. Feedback & Status

**The "Silent Failure" Risk**
*   **The Flaw:** Virus detections (Quarantine) happen in a background tab without global notification.
*   **The Friction:** Users might assume all is well while infected files are sitting in another tab.
*   **The Fix:** **Notification Badges** in the Nav Rail. e.g., `Quarantine (3)` in red text.

---

## Roadmap

### Phase 1: Low Hanging Fruit
1.  Rename "Hopper" -> "Local".
2.  Implement Persistent Keybinding Footer.
3.  Add Notification Badges to Rail.

### Phase 2: Structural Refactor
5.  Side-by-side Local/Remote "Commander" layout.

### Phase 3: Visual Polish
6.  Inactive panel dimming logic.
7.  Mouse hover/pre-light states.
