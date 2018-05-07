var stompClient = null;

function connect() {
    var socket = new SockJS('/websocket');
    stompClient = Stomp.over(socket);
    stompClient.connect({}, function (frame) {
        console.log('Connected: ' + frame);
        stompClient.subscribe('/topic/workers', function (workersDetails) {
            showWorkersDetails(JSON.parse(workersDetails.body));
        });
        stompClient.subscribe('/topic/logs', function (log) {
            showLogs(JSON.parse(log.body));
        });
    });
}

function disconnect() {
    if (stompClient !== null) {
        stompClient.disconnect();
    }
    console.log("Disconnected");
}

function showWorkersDetails(workersDetails) {
    $("#workers").html("");
    for (var i = 0; i < workersDetails.length; i++) {
        var worker = workersDetails[i];
        $("#workers").append("<tr><td>" + worker.workerId + "</td><td>" + worker.host + ":" + worker.port + "</td></tr>");
    }
}

function showLogs(log) {
    $("#logs").append("<tr><td>" + log.time + "</td><td>" + log.level + "</td><td>" + log.message + "</td></tr>");
}

function deploy() {
    var graphDefinition = $("#graphDefinition").get(0);
    var errorMessage = $("#errorMessage:first");

    if (graphDefinition.files.length === 0) {
        errorMessage.html("No file selected");
        console.log("No file selected");
        return;
    }

    errorMessage.html("");
    console.log("Deploying graph");
    var reader = new FileReader();
    reader.onload = (function(reader)
    {
        return function()
        {
            console.log(reader.result);
            stompClient.send("/app/deploy", {}, JSON.stringify(reader.result));
        }
    })(reader);
    reader.readAsText(graphDefinition.files[0]);
}

$(function () {
    connect();
});
