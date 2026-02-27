const form = document.getElementById('sim-form');
const resultCard = document.getElementById('result');
const jsonOut = document.getElementById('json');
const leagueSelect = document.getElementById('league');
const dateInput = document.getElementById('date');

// Set default date (today, local)
(function setToday() {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const dd = String(now.getDate()).padStart(2, '0');
  dateInput.value = `${yyyy}-${mm}-${dd}`;
})();

// Load leagues from backend
async function loadLeagues() {
  try {
    const res = await fetch('/api/league-list');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const leagues = await res.json();

    // Clear and populate options
    leagueSelect.innerHTML = '';
    if (!Array.isArray(leagues) || leagues.length === 0) {
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = 'No leagues found (seed missing)';
      opt.disabled = true;
      opt.selected = true;
      leagueSelect.appendChild(opt);
      return;
    }

    // Add options sorted by name
    leagues
      .sort((a, b) => (a.name || a.code).localeCompare(b.name || b.code))
      .forEach(({ code, name }) => {
        const opt = document.createElement('option');
        opt.value = code;
        opt.textContent = name ? `${name} (${code})` : code;
        leagueSelect.appendChild(opt);
      });
  } catch (err) {
    // Fallback single option on error
    leagueSelect.innerHTML = '';
    const opt = document.createElement('option');
    opt.value = '';
    opt.textContent = 'Error loading leagues';
    opt.disabled = true;
    opt.selected = true;
    leagueSelect.appendChild(opt);
    console.error('Failed to load leagues:', err);
  }
}

document.addEventListener('DOMContentLoaded', loadLeagues);

// Submit handler
form.addEventListener('submit', async (e) => {
  e.preventDefault();

  const teamA = document.getElementById('teamA').value.trim();
  const teamB = document.getElementById('teamB').value.trim();
  const date = dateInput.value;   // YYYY-MM-DD
  const league = leagueSelect.value;

  if (!teamA || !teamB || !date || !league) {
    alert('Please fill in all fields.');
    return;
  }

  const payload = {
    team_a: teamA,
    team_b: teamB,
    date: date,
    league_code: league
  };

  jsonOut.textContent = 'Running...';
  resultCard.classList.remove('hidden');

  try {
    const res = await fetch('/api/simulate', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    jsonOut.textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    jsonOut.textContent = `Error: ${err.message || err}`;
  }
});
