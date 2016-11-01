@echo OFF
java -Dsbe.validation.stop.on.error=true -Dsbe.output.dir=../src-gen -cp .;../lib/* uk.co.real_logic.sbe.SbeTool Helios-MMB.xml