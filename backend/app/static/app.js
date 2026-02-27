const form = document.getElementById('sim-form');
const resultCard = document.getElementById('result');
const jsonOut = document.getElementById('json');

form.addEventListener('submit', async (e) => {
  e.preventDefault();

  const teamA = document.getElementById('teamA').value.trim();
  const teamB = document.getElementById('teamB').value.trim();
  const date = document.getElementById('date').value;   // ISO YYYY-MM-DD
  const league = document.getElementById('league').value;

  if (!teamA || !teamB || !date || !league) {
    alert('Please fill in all fields.');
    return;
  }

  // Build payload (your backend accepts ISO date without time)
  const payload = {
    team_a: teamA,
    team_b: teamB,
    date: date,              // e.g. "2026-02-10"
    league_code: league
  };

  // Call the backend on the same domain
  const url = '/api/simulate';

  jsonOut.textContent = 'Running...';
  resultCard.classList.remove('hidden');

  try {
    const res = await fetch(url, {
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
