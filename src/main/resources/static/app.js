var stompClient = null;

function connect() {
    var socket = new SockJS('/websocket');
    stompClient = Stomp.over(socket);
    stompClient.connect({}, function (frame) {
        console.log('Connected: ' + frame);
        stompClient.subscribe('/topic/operators', function (operatorsDetails) {
            showOperatorsDetails(JSON.parse(operatorsDetails.body));
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

function showOperatorsDetails(operatorsDetails) {
    $("#operators").html("");
    for (var i = 0; i < operatorsDetails.length; i++) {
        var operator = operatorsDetails[i];
        $("#operators").append(
            "<tr class=\" " + getContext(operator.state) + "\">" +
            "<td>" + operator.registrationId + "</td>" +
            "<td>" + operator.host + ":" + operator.port + "</td>" +
            "<td>" + operator.state + "</td>" +
            "<td>" + prettyPrint(operator.operatorId) + "</td>" +
            "<td>" + prettyPrint(operator.windowSlide) + "</td>" +
            "<td>" + prettyPrint(operator.windowSize) + "</td>" +
            "<td>" + prettyPrint(operator.aggregation) + "</td>" +
            "</tr>");
    }
}

function prettyPrint(value) {
    return value == null ? "-" : value;
}

function getContext(state) {
    if (state === "FREE") {
        return "success";
    } else if (state === "READY" || state === "INFO") {
        return "info";
    } else if (state === "BUSY" || state === "WARN") {
        return "warning";
    } else if (state === "CRUSHED" || state === "SEVERE" ) {
        return "danger";
    } else {
        return "default";
    }
}

function showLogs(log) {
    $("#logs").prepend(
        "<tr class=\"" + getContext(log.level) + "\">" +
        "<td>" + log.time + "</td>" +
        "<td>" + log.level + "</td>" +
        "<td>" + log.message + "</td>" +
        "</tr>");
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
    reader.onload = (function (reader) {
        return function () {
            console.log(reader.result);
            stompClient.send("/app/deploy", {}, JSON.stringify(reader.result));
        }
    })(reader);
    reader.readAsText(graphDefinition.files[0]);
}

$(function () {
    connect();
});
