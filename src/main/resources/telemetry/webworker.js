self.importScripts('js/viz-lite.js');

function httpGet(theUrl, onSuccess, onError) {
  var xmlHttp = new XMLHttpRequest();
  xmlHttp.timeout = 2000;
  xmlHttp.onreadystatechange = function() {
    if (this.readyState === 4) {
      if (this.status >= 200 && this.status < 300) {
        onSuccess(this.responseText);
      } else{
        onError("Received " + this.status + " from server.");
      }
    }
  };
  xmlHttp.open( "GET", theUrl, true );
  xmlHttp.send();
}

var worker = this;

this.onmessage = function onmessage(){
  httpGet('graph.dot', function(result){
    var updateDemo = Viz(result, {format: "svg", engine: "dot"});
    worker.postMessage({result: updateDemo});
  }, function(error) {
    console.log('Error: ' + error);
  })
};

