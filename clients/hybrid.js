function initialize() {
    var app = new Vue({
        el: '#ticker',
        data: {
        items: [
            { message: 'Ready' }
        ]
        }
    })
    if ("WebSocket" in window) {
        var ws = new WebSocket("ws://localhost:8080/notify");
        ws.onopen = function() {
            console.log("onopen");
        };
        ws.onmessage = function (evt) { 
            var notif = JSON.parse(evt.data);
            // console.log(notif);
            if (app.items.length > 9) {
                app.items.pop();
            }
            app.items.unshift(notif);

        };
        ws.onclose = function() {                   
            console.log("onclose"); 
        };
    } else {
        console.log("WebSocket not in window");
    }
}
