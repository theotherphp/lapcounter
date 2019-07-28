function initialize() {
    var app = new Vue({
        el: '#ticker',
        data: {
            notifications: []
        }
    })
    if ("WebSocket" in window) {
        var ws = new ReconnectingWebSocket("ws://mini.local:8080/notify");
        ws.onopen = function() {
            console.log("onopen");
        };
        ws.onmessage = function (evt) { 
            var notif = JSON.parse(evt.data);
            for (var i = 0; i < app.notifications.length; i++) {
                if (notif.TagID == app.notifications[i].TagID) {
                    app.notifications.splice(i, 1);
                    break;
                }
            }
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
