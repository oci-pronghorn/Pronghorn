# Telemetry Web UI

Steps to run
1. cd to top of Pronghorn project
2. mvn clean install
3. mvn exec:java -Dexec.addResourcesToClasspath=true -Dexec.mainClass="com.ociweb.pronghorn.TelemetryTestTool -DskipTests"
4. browse any of the three URLs output after "Telemetry Server is now ready on"
