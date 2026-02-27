const form = document.getElementById('sim-form');
const resultCard = document.getElementById('result');
const jsonOut = document.getElementById('json');
const leagueSelect = document.getElementById('league');
const dateInput = document.getElementById('date');

// --------------------------------------------------------
// Set default date = today (local)
// --------------------------------------------------------
(function setToday() {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const dd = String(now.getDate()).padStart(2, '0');
  dateInput.value = `${yyyy}-${mm}-${dd}`;
})();


// --------------------------------------------------------
// Load leagues from backend
// --------------------------------------------------------
async function loadLeagues() {
  try {
    const res = await fetch('/api/league-list');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const leagues = await res.json();

    leagueSelect.innerHTML = '';

    leagues
      .sort((a, b) => (a.name || a.code).localeCompare(b.name || b.code))
      .forEach(({ code, name }) => {
        const opt = document.createElement('option');
        opt.value = code;
        opt.textContent = `${name} (${code})`;
        leagueSelect.appendChild(opt);
      });
  } catch (err) {
    console.error('Failed loading leagues:', err);
    leagueSelect.innerHTML = '<option>Error loading leagues</option>';
  }
}

document.addEventListener('DOMContentLoaded', loadLeagues);


// --------------------------------------------------------
// Auto-resolve league from team names
// --------------------------------------------------------
let resolveTimeout = null;

async function resolveLeagueIfPossible() {
  clearTimeout(resolveTimeout);

  resolveTimeout = setTimeout(async () => {
    const teamA = document.getElementById('teamA').value.trim();
    const teamB = document.getElementById('teamB').value.trim();
    if (!teamA || !teamB) return;

    const url = `/api/resolve-league?team_a=${encodeURIComponent(teamA)}&team_b=${encodeURIComponent(teamB)}`;

    try {
      const res = await fetch(url);
      const data = await res.json();
      console.log('resolver:', data);

      // GREEN: league resolved cleanly
      if (data.resolved && data.league_code) {
        leagueSelect.value = data.league_code;
        leagueSelect.style.borderColor = '#10b981'; // green
      } 
      // YELLOW: suggestions or fuzzy match only
      else {
        leagueSelect.style.borderColor = '#f59e0b'; // amber
        console.log('Suggestions:', data.suggestions);
      }
    } catch (err) {
      console.error('League resolve failed:', err);
    }
  }, 200); // debounce 200 ms
}

// Trigger resolution on input/blur
document.getElementById('teamA').addEventListener('input', resolveLeagueIfPossible);
document.getElementById('teamB').addEventListener('input', resolveLeagueIfPossible);
document.getElementById('teamA').addEventListener('blur', resolveLeagueIfPossible);
document.getElementById('teamB').addEventListener('blur', resolveLeagueIfPossible);


// --------------------------------------------------------
// Submit simulation
// --------------------------------------------------------
form.addEventListener('submit', async (e) => {
  e.preventDefault();

  const teamA = document.getElementById('teamA').value.trim();
  const teamB = document.getElementById('teamB').value.trim();
  const date = dateInput.value;
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
