set JAVA_OPTS=-Xmx2G
set JAVA_OPTS=-Xss2G

sbt "set test in assembly := {}" clean assembly