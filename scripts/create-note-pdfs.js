function run(argv) {

	if (argv.length < 1) {
		console.log("Usage: osascript -l JavaScript create-note-pdfs.scpt /powerpoint/file/path/pptfile.pptx")
		return
	}

	var filepath = argv[0]
	var pathArray = argv[0].split('/')
	var filename = pathArray[pathArray.length - 1]
	var name = filename.split('.')[0]
	const pptVersion = '16.70 (23021201)'

	// console.log(`This version of the script was tested on Microsoft PowerPoint version ${pptVersion}`)
	console.log(`Processing: ${filename}`)

	var ppt = Application('Microsoft PowerPoint')
	ppt.includeStandardAdditions = true
	ppt.activate()

	ppt.open(Path(filepath))

	var se = Application('System Events')
	var pptProc = se.processes['PowerPoint']

	try {
		// Create student slides without notes
		// Use System Events to drive the PowerPoint GUI and select "Export..."

		pptProc.menuBars[0].menuBarItems['File'].menus['File'].menuItems['Export...'].click()
		delay(5)

		pptProc.windows[name].sheets[0].textFields[0].setProperty('value', name + '-student.pdf')
		pptProc.windows[name].sheets[0].buttons['Export'].click()
		delay(2)

		// Create instructor slides with notes - This first time generates "Rubbish" files because of
		// a bug in PowerPoint.  The same routine will run again below to generate the real file.

		// Use System Events to drive the PowerPoint GUI and select "Print -> Save As PDF"

		pptProc.menuBars[0].menuBarItems['File'].menus['File'].menuItems['Print...'].click()

		delay(2)

		pptProc.windows[name].sheets[0].splitterGroups[0].scrollAreas[1].groups[1].groups[0].popUpButtons[1].click()
		while (pptProc.windows[name].sheets[0].splitterGroups[0].scrollAreas[1].groups[1].groups[0].popUpButtons[1].menus[0].menuItems['Notes'] === null) { delay(.1) }
		pptProc.windows[name].sheets[0].splitterGroups[0].scrollAreas[1].groups[1].groups[0].popUpButtons[1].menus[0].menuItems['Notes'].click()

		delay(3)

		// Click PDF Button
		pptProc.windows[name].sheets[0].splitterGroups[0].groups[1].buttons[0].click()
		pptProc.windows[name].sheets[0].sheets[0].textFields[0].setProperty('value', name + '-rubbish.pdf')
		pptProc.windows[name].sheets[0].sheets[0].buttons['Save'].click()

		// Create instructor slides with notes
		// Use System Events to drive the PowerPoint GUI and select "Print -> Save As PDF"

		pptProc.menuBars[0].menuBarItems['File'].menus['File'].menuItems['Print...'].click()

		delay(2)

		pptProc.windows[name].sheets[0].splitterGroups[0].scrollAreas[1].groups[1].groups[0].popUpButtons[1].click()
		while (pptProc.windows[name].sheets[0].splitterGroups[0].scrollAreas[1].groups[1].groups[0].popUpButtons[1].menus[0].menuItems['Notes'] === null) { delay(.1) }
		pptProc.windows[name].sheets[0].splitterGroups[0].scrollAreas[1].groups[1].groups[0].popUpButtons[1].menus[0].menuItems['Notes'].click()

		delay(3)

		// Click PDF Button
		pptProc.windows[name].sheets[0].splitterGroups[0].groups[1].buttons[0].click()
		pptProc.windows[name].sheets[0].sheets[0].textFields[0].setProperty('value', name + '-instructor.pdf')
		pptProc.windows[name].sheets[0].sheets[0].buttons['Save'].click()
	}
	catch(error) {
		try {
			pptProc.windows[name].sheets[0].buttons['Cancel'].click()
		}
		catch(error) {
			console.log('No Cancel button to click.')
		}
		console.log(`Failed to process file: ${filename}`)
	}

	delay(5)

	try {
		ppt.quit({saving:'no'})
	}
	catch(error) {
		console.log('Failed to quit PowerPoint: ' + error)
	}

	delay(10)
}
