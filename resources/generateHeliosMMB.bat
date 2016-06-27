@echo OFF
@set JAVA_HOME=C:\Program Files\Zulu\zulu-8\bin
"%JAVA_HOME%\java" -Dsbe.validation.stop.on.error=true -Dsbe.output.dir=../src-gen -cp .;../lib/* uk.co.real_logic.sbe.SbeTool Helios-MMB.xml