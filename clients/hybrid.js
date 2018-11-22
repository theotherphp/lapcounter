function initialize() {
    var app = new Vue({
        el: '#ticker',
        data: {
            notifications: []
        }
    })
    if ("WebSocket" in window) {
        var ws = new WebSocket("ws://mini.local:8080/notify");
        ws.onopen = function() {
            console.log("onopen");
        };
        ws.onmessage = function (evt) { 
            var notif = JSON.parse(evt.data);
            if (app.notifications.length > 12) {
                app.notifications.pop();
            }
            app.notifications.unshift(notif);

        };
        ws.onclose = function() {                   
            console.log("onclose"); 
        };
    } else {
        console.log("WebSocket not in window");
    }
}
