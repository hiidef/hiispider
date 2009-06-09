
pep8="python pep8.py";
pylint="/opt/local/Library/Frameworks/Python.framework/Versions/2.6/bin/pylint --method-rgx=[a-z_][A-Za-z0-9_]{2,60}$ --disable-msg=C0111 --good-names=d";

rm -rf pylint
rm -rf pep8

directories=$(find  "../awspider" -type d \! -path "*.git*" )
for directory in $directories
do
	pythonFilesCount=$(find $directory -type f -path "*.py" \! -path "*.pyc" | wc -l)
	if [ $pythonFilesCount -gt 0 ]; then
		newdir=$(echo $directory | sed "s/\.\.\/awspider//")
		mkdir "pep8$newdir"
		mkdir "pylint$newdir"
	fi
done

pythonFiles=$(find "../awspider" -type f -path "*.py" \! -path "*.pyc")
for pythonFile in $pythonFiles
do
	outputFile=$(echo $pythonFile | sed "s/\.\.\/awspider//")
	
	echo "pep8: $pythonFile"
	$pep8 $pythonFile > "pep8$outputFile.txt"
	
	echo "pylint: $pythonFile"
	$pylint $pythonFile > "pylint$outputFile.txt"
done