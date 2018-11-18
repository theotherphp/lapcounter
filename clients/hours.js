// Populates the table rows of hours.html based on team hours retrieved from the server

// Look at the Hours bitfield to see which hours this team has been on the track
// LSB is hour 0, aka 9 AM. MSB is hour 23, aka 8 AM the next morning
function insertOneTeamHours(tbl, team) {
    var row = tbl.insertRow(tbl.rows.length);
    var count = 0;
    for (var i = 0; i < 24; i++) {
        var cell = row.insertCell(i);
        if ((team.Hours & 1<<i) > 0) {
            cell.classList.add("on-track");
            count++;
        }
    }
    row.insertCell(0).innerHTML = count.toString();
    var teamCell = row.insertCell(0);
    teamCell.classList.add("team-name");
    teamCell.innerHTML = team.Name;

}

// Get the list of teams from the server
function insertAllTeamHours() {
    var req = new XMLHttpRequest();
    req.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            var teams = JSON.parse(this.responseText);
            var tbl = document.getElementById("hours-by-team");
            for (var i = 0; i < teams.length; i++) {
                insertOneTeamHours(tbl, teams[i]);
            }
        }
    };
    req.open("GET", "/teamsx", true);
    req.send();
}
