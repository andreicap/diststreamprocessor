var stompClient = null;

function connect() {
    var socket = new SockJS('/websocket');
    stompClient = Stomp.over(socket);
    stompClient.connect({}, function (frame) {
        console.log('Connected: ' + frame);
        stompClient.subscribe('/topic/machines', function (machinesDetails) {
            showAvailableMachinesDetails(JSON.parse(machinesDetails.body));
        });
    });
}

function disconnect() {
    if (stompClient !== null) {
        stompClient.disconnect();
    }
    console.log("Disconnected");
}

// function deploy() {
//     stompClient.send("/app/deploy", {}, JSON.stringify({'name': $("#name").val()}));
// }

function showAvailableMachinesDetails(machinesDetails) {
    $("#machines").html("");
    for (var i = 0; i < machinesDetails.length; i++) {
        var machine = machinesDetails[i];
        $("#machines").append("<tr><td>" + machine.host + ":" + machine.port + "</td></tr>");
    }
}

$(function () {
    connect();
});
