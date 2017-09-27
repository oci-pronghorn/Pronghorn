self.importScripts('viz-lite.js');

function httpGet(theUrl) {
    var xmlHttp = null;

    xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "GET", theUrl, false );
    xmlHttp.send( null );

    return xmlHttp.responseText;
}

this.onmessage = function (e) {
    var dot = httpGet("graph.dot");
    var updateDemo = Viz(dot, {format: "svg", engine: "dot"});
    this.postMessage({result: updateDemo});
};
