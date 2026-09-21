extends Node

#####used if using a direct call and an api key
var GEMINI_API_KEY #used if using a direct call and an api key
var GEMINI_PRO_URL: Array[String] = [] #used if using a direct call and an api key
var GEMINI_URL #used if using a direct call and an api key
var gcp_token = "" #used if using a direct call and an api key
var is_gemini_error = false
var current_gemini_model = 0
#####

#### Configuration of the bridge API
var base_url = "https://rtr-backend-fvrfj3pqja-uc.a.run.app"

var lang = "English" 


###connect on all the messages
func _ready():
	
	if SignalBus.language == "JP": lang = "Japanese"
	
	
	##connect with the SignalBus
	#_on_api_key(SignalBus.gemini_api_key) #used if using a direct call and an api key
	#SignalBus.gemini_api_key_received.connect(_on_api_key) #used if using a direct call and an api key
	
	SignalBus.gemini_help_requested.connect(_on_need_gemini_help)
	SignalBus.gemini_backstory_requested.connect(_on_get_gemini_backstory)
	SignalBus.gemini_backstory_image_requested.connect(call_api_bridge_generate_backstory_image)
	SignalBus.send_screenshot_to_gcs.connect(_on_send_screenshots_to_gcs)
	SignalBus.screen_state.connect(_on_game_over)
	

####get rank on game over
func _on_game_over(state):
	if state == SignalBus.GAMEOVER: #kick in only if we have finished
		#retrieve rank
		var http_request  = HTTPRequest.new()
		add_child(http_request)
		http_request.request_completed.connect(_on_rank_received.bind(http_request))
		var connection = base_url + "/get_rank"
		var headers = ["Content-Type: application/json"]
	
		var body = JSON.stringify({
			"session_id": SignalBus.session_id,
		})
		http_request.request(connection, headers, HTTPClient.METHOD_POST, body) 
		
		#retrieve gemini summary
		var http_request2  = HTTPRequest.new()
		add_child(http_request2)
		http_request2.request_completed.connect(_on_gemini_summary_received.bind(http_request2))
		var connection2 = base_url + "/get_gemini_summary"
		http_request2.request(connection2, headers, HTTPClient.METHOD_POST, body) 
	
func _on_rank_received(result: int, _response_code: int, _headers: PackedStringArray, body: PackedByteArray, _req_node : HTTPRequest = null):
	if result != OK:
			printerr("BigQuery: failed to retrieve rank for this session")
	else:
		var rank = body.get_string_from_utf8()
		SignalBus.session_rank_received.emit(rank)

func _on_gemini_summary_received(result: int, _response_code: int, _headers: PackedStringArray, body: PackedByteArray, _req_node : HTTPRequest = null):
	if result != OK:
			printerr("BigQuery: failed to retrieve the summary for this session")
	else:
		var summary = body.get_string_from_utf8()
		SignalBus.gemini_summary_received.emit(summary)
###

###send regularly send screenshots of the game to GCS
func get_screenshot() -> Image:
	return get_viewport().get_texture().get_image()

func _on_send_screenshots_to_gcs():
	SignalBus.last_screenshot_timestamp = Time.get_datetime_string_from_system()
	#print("Sending Screenshot")
	if SignalBus.SEND_SCREENSHOTS:
		await get_tree().process_frame
		await RenderingServer.frame_post_draw
		#var sub_viewport =  $/root/Game/GameManager/level_1_screen/GameView/ViewportDisplay2
		#var capture = sub_viewport.get_texture().get_image()
		var capture = get_viewport().get_texture().get_image()
		capture.resize(640, 360, Image.INTERPOLATE_BILINEAR) #reduce the size
		
		
		#get_viewport().get_texture().get_image()
		var buffer = capture.save_png_to_buffer()
		var base64_string = Marshalls.raw_to_base64(buffer)
		var body = JSON.stringify({
			"image": "data:image/png;base64," + base64_string,
			"session_id": SignalBus.session_id,
			"timestamp_seconds": SignalBus.last_screenshot_timestamp
		})
		
		# Make the HTTP request
		var http_request  = HTTPRequest.new()
		
		add_child(http_request)
		http_request.request_completed.connect(_on_send_screenshots_to_gcs_request_completed.bind(http_request))
		http_request.request(
			base_url + "/publish_screenshot_image",
			["Content-Type: application/json"],
			HTTPClient.METHOD_POST,
			body
		)
		
		
			#save to file
		var  directory_name = "screenshots"
		if !DirAccess.dir_exists_absolute("user://" + directory_name):
			var error = DirAccess.make_dir_recursive_absolute("user://" + directory_name)
			if error != OK:
				printerr("Failed to create directory. Error code: ", error)
		
		var filename = "user://"+directory_name+"/"+str(SignalBus.session_id)+"screenshot-{0}.png".format({"0":SignalBus.last_screenshot_timestamp})
		capture.save_png(filename)
		SignalBus.last_screenshot = filename

#receive result result: int, _response_code: int, _headers: PackedStringArray, body: PackedByteArray, _req_node : HTTPRequest = null
func _on_send_screenshots_to_gcs_request_completed(result: int, _response_code: int, _headers: PackedStringArray, body: PackedByteArray, _req_node : HTTPRequest = null):
	if result != HTTPRequest.RESULT_SUCCESS:
		printerr("Screenshot failure - HTTP Request failed with error: ", result)
		return
	
	if _response_code == 200:
		var json = JSON.parse_string(body.get_string_from_utf8())
		#print("Upload successful! Response: ", json)
	else:
		printerr("Upload failed with response code: ", _response_code)
		printerr("Response body: ", body.get_string_from_utf8())
####		

###request support from Gemini sending a screenshot + prompt
func _on_need_gemini_help():
	
	var base64_image = ""
	if FileAccess.file_exists(SignalBus.last_screenshot):
		var image = Image.load_from_file(SignalBus.last_screenshot)
	# 1. Encode the image to base64.
		var image_bytes = image.save_png_to_buffer() # Use PNG for best results with Gemini
		base64_image = Marshalls.raw_to_base64(image_bytes)	
	call_adk_agent_with_prompt_and_image(base64_image)

##########################################Signals handling & call preparation	

#using answers from the user ask gemini to generate a story about the datacenter
#using the backstory ask imagen3 gemini for a background image 
func _on_get_gemini_backstory():
	
	#Ask for an image
	var prompt_img = "Illustrate the following story using a 16 bits retro style design with neon glows using google color palette:\n"
	prompt_img += " You have been tasked to clean a datacenter that have been overtaken by old technologies from the 80s and 90s like matrix printers & CRTs and fyling floppy disks.\n"
	prompt_img += "Make sure to incorporate in your image elements that remind of a: "+SignalBus.trivia_result[0]['a']+"\n"
	prompt_img += " and an overall vibe: "+SignalBus.trivia_result[1]['a']+"\n"	
	call_api_bridge_generate_backstory_image(prompt_img)
	
	#ask for a compelling story
	var prompt = "You are a video game story writer and you are tasked to write a very short backstory of the game to make the player want to play.\n" 
	prompt += "The game takes place in an old datacenter from the 80s or 90s swarming with old technologies like CRTs, old matrix dot printers or floppy disks." 
	prompt += "The hero of the story is tasked to clean the datacenter from thos antiquities and find the boss, an old angry ATX Server that controls the old tech and hide in the datacenter.\n" 
	prompt += "To tailor the story to the player, he gave you the following indications on his personality through a Q&A session." 
	
	for i in SignalBus.trivia_result.size():
		prompt +="Question:" + SignalBus.trivia_result[i]['q'] +", Answer:"+ SignalBus.trivia_result[i]['a'] + ".\n"	
	prompt += "Color your story with element from his answers and keep the story short, Write the story in the following language: "+lang
	call_api_bridge_generate_backstory_story(prompt)
	
##########################################API Integration	

####Connect to a local API Bridge

####connect to the api bridge to get the backstory image
func call_api_bridge_generate_backstory_image(prompt):
	var http_request  = HTTPRequest.new()
	add_child(http_request)
	http_request.request_completed.connect(_on_call_api_bridge_backstory_image_request_completed.bind(http_request))
	var connection = base_url + "/get_backstory_image"
	var headers = ["Content-Type: application/json"]
	
	var body = JSON.stringify({
			"session_id": SignalBus.session_id,
			"prompt": prompt
		})
	
	http_request.request(connection, headers, HTTPClient.METHOD_POST, body) 

#Got the image that we need to display
func _on_call_api_bridge_backstory_image_request_completed(result: int, _response_code: int, _headers: PackedStringArray, body: PackedByteArray, _req_node : HTTPRequest = null):
	if result != OK:
			printerr("Imagen3: failed to generate image")
	else:
		var base64_image = body.get_string_from_utf8()
		#print(base64_image)
		SignalBus.gemini_backstory_image = base64_image
		SignalBus.gemini_backstory_image_received.emit()
####

####connect locally to send analytic sensors data
func call_api_bridge_analytics(json_string):
	var http_request  = HTTPRequest.new()
	add_child(http_request)
	http_request.request_completed.connect(_on_call_api_bridge_request_completed.bind(http_request))
	var connection = base_url + "/backendcomm"
	var headers = ["Content-type: application/json"]
	http_request.request(connection, headers, HTTPClient.METHOD_POST, json_string) 
####Receiving analytics registration answer from the API Bridge
func _on_call_api_bridge_request_completed(result: int, _response_code: int, _headers: PackedStringArray, _body: PackedByteArray, _req_node : HTTPRequest = null):
	if result != OK:
			printerr("Pub/Sub: failed to send analytic data")
####

#### go through the API bridge to call Gemini and get a backstory
func call_api_bridge_generate_backstory_story(prompt:String):
	var body =  JSON.stringify({
			"prompt":prompt
		})
	# Make the HTTP request
	var http_request  = HTTPRequest.new()
	add_child(http_request)
	http_request.request_completed.connect(_on_call_api_bridge_generate_backstory_story_request_completed.bind(http_request))
	http_request.request(
			base_url + "/get_backstory_story",
			["Content-Type: application/json"],
			HTTPClient.METHOD_POST,
			body
		)
	SignalBus.gemini_backstory_requested_details.emit(prompt)	
###Result received from the API bridge with a backstory
func _on_call_api_bridge_generate_backstory_story_request_completed(result: int, _response_code: int, _headers: PackedStringArray, body: PackedByteArray, _req_node : HTTPRequest = null):
	if result != OK:
		printerr("Error receiving gemini backstory story generation")
	else:
		var content = body.get_string_from_utf8()
		print(content)
		SignalBus.gemini_backstory_received.emit()
		SignalBus.gemini_backstory_text = content
####
				
####Call ADK agent with prompt & an image
func call_adk_agent_with_prompt_and_image(base64_image:String):
	var body = JSON.stringify({
			"image": "data:image/png;base64," + base64_image,
			"session_id": SignalBus.session_id,
			"stopwatch": SignalBus.last_screenshot_timestamp,
			"has_weapon1":!SignalBus.players[0].weapons[1]['disabled'],
			"has_weapon2":!SignalBus.players[0].weapons[2]['disabled'],
			"hit_count": SignalBus.players[0].hit_count,
			"health": SignalBus.players[0].health,
			"score": SignalBus.score,
			"game_difficulty":SignalBus.game_difficulty,
			"language": lang
		})
		
	# Make the HTTP request
	var http_request  = HTTPRequest.new()
	add_child(http_request)
	http_request.request_completed.connect(_on_call_adk_agent_with_prompt_and_image_request_completed.bind(http_request))
	http_request.request(
			base_url + "/get_agent_help",
			["Content-Type: application/json"],
			HTTPClient.METHOD_POST,
			body
		)

func _on_call_adk_agent_with_prompt_and_image_request_completed(result: int, _response_code: int, _headers: PackedStringArray, body: PackedByteArray, _req_node : HTTPRequest = null):
	if result != OK:
			printerr("ADK: failed to receive help from agent")
	else:
		var parsedJSON = JSON.parse_string(body.get_string_from_utf8())
		
		SignalBus.gemini_help_received.emit(parsedJSON["help"]+" \n Difficulty Level: "+str(parsedJSON["difficulty_level"]))
					
		if parsedJSON.has("difficulty_level"):
			#set difficulty
			SignalBus.prev_game_difficulty = SignalBus.game_difficulty	
			if parsedJSON["difficulty_level"] == 2:
				#print("hard")
				SignalBus.game_difficulty = SignalBus.HARD
			elif parsedJSON["difficulty_level"] == 1:
				#print("med")
				SignalBus.game_difficulty = SignalBus.MEDIUM
			else:		
				#print("easy")
				SignalBus.game_difficulty = SignalBus.EASY
			SignalBus.gemini_difficulty_adjusted.emit(parsedJSON["difficulty_level"], parsedJSON['reason'])		
####

####################Calling directly Gemini / Imagen from the game itself####################

const IMAGEN3_URL ="https://{region}-aiplatform.googleapis.com/v1/projects/{project_id}/locations/{region}/publishers/google/models/imagen-3.0-generate-002:predict"

# we expect getting a key that could be loaded from the parameter files of the game (signal exists but has been commented)
func _on_api_key(key):
	GEMINI_API_KEY = key
	#backstory
	GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=" + GEMINI_API_KEY
	
	#Help
	GEMINI_PRO_URL.append("https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro-preview-03-25:generateContent?key=" + GEMINI_API_KEY)
	GEMINI_PRO_URL.append("https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=" + GEMINI_API_KEY)


####Call gemini directly from our game to get a backstory
func call_gemini_backstory(prompt:String) -> String:
	var http_request  = HTTPRequest.new()
	add_child(http_request)
	http_request.request_completed.connect(_on_call_gemini_backstory_request_completed.bind(http_request))
	var connection = GEMINI_URL
	var post_data = '{
  "contents": [{
	"parts":[{"text":"'+prompt+'"}]
	}]
   }'
	#var json_data = JSON.print(post_data) #convert dictionary to json string
	
	SignalBus.gemini_backstory_requested_details.emit(prompt)
	
	var headers = ["Content-Type: application/json"] #set header
	http_request.request(connection, headers, HTTPClient.METHOD_POST, post_data)
	return ""
func _on_call_gemini_backstory_request_completed(result: int, _response_code: int, _headers: PackedStringArray, body: PackedByteArray, _req_node : HTTPRequest = null):
	if result != OK:
		printerr("Error receiving gemini backstory")
	else:
		var dict_body = JSON.parse_string(body.get_string_from_utf8())
		if dict_body.has("candidates") and \
			dict_body.candidates is Array and \
			dict_body.candidates.size() > 0 and \
			dict_body.candidates[0].has("content") and \
			dict_body.candidates[0].content.has("parts") and \
			dict_body.candidates[0].content.parts.size() > 0:
			#print(dict_body.candidates[0].content.parts[0].text)
			
			SignalBus.gemini_backstory_received.emit()
			SignalBus.gemini_backstory_text = dict_body.candidates[0].content.parts[0].text
			
		else:
			printerr("Error accessing gemini for the backstory")
#######

###request support from Gemini sending a screenshot + prompt, direct call
func _on_need_gemini_help_direct():
		
	var prompt_text = "You are assisting a user who is playing a video game that takes place in an old datacenter where old technologies like CRTs and Printers have taken over and he is task to go and clean it \n."
	
	##############Context
	
	###############Player stats
	prompt_text += "Player has been playing for "+SignalBus.get_stopwatch()+"\n" 
	prompt_text += "Player health level is "+str(SignalBus.players[0].health)+" out of 200 \n"
	prompt_text += "Player has been hit "+str(SignalBus.players[0].hit_count)+" times.\n" 
	prompt_text += "Player score is "+str(SignalBus.score)+".\n"
	prompt_text += "Player current difficulty level is "+str(SignalBus.game_difficulty)+"\n"
	
	##############Weapons
	var has_blaster = !SignalBus.players[0].weapons[1]['disabled']
	var has_gauntlet = !SignalBus.players[0].weapons[2]['disabled']
	
	#user has 2 weapons
	if has_blaster and has_gauntlet:
		#You know that the boss is located on the top right side of the level.
		prompt_text += "The player has both tools and is ready to go meet his destiny\n"
		prompt_text += "Give instructions to the player on how to get to the large room located on the top right where the final boss is located.\n"
		prompt_text += "explain that to defeat the final boss you should use the gauntlet 2000 to create a barrier and therefore avoid being hit by floppy disks.\n"
		prompt_text += "3.5 floppy disks are faster but inflict less damage, larger 5.25 are slower but inflict more damage"
		prompt_text += "moving around would give him enough time to use the blaster  cleaner 80 to shoot at the boss. The boss has 200 of health and can be identified by a blue square"
		
	elif has_blaster:
		#user has the blaster only
		prompt_text += "The player already has the blaster cleaner 80 but will need to protect himself more if he wants to defeat the final boss.\n"
		prompt_text += "Recommend to the player to go fetch the weapon on the very far right represented by a yellow square. \n"
	elif has_gauntlet:
		#user has the gauntlet
		prompt_text += "The player already has the gauntlet 2000 a power protective tool against various tech notably floppy disks.\n"
		prompt_text += "Recommend to the player to also retrieve a tool for the offense, the mighty blaster 80.\n" 
	else:
		#user has no weapon
		prompt_text += "The player does not currently have any tools to defend itself or get rid of the old techs \n"
		prompt_text += "Recommend to the player to go fetch the weapon closest to where the player started the level which is where the blaster cleaner 80 is located, on the map represented by a yellow square. \n"
	
	prompt_text += "Based on the screenshot of the game, the mini map in the screenshot and the information you have, give short and concise instructions to the user so that they know what to do"
		
	prompt_text += "If you think you should not help just yet just crack a joke or give a fun fact and keep your explaination fuzzy." 
	
	prompt_text += "Prepare your answer with the textual help for the first field, a second field that represents the difficulty level of the game from 0 to 2 with 0 being easy and 2 being hard."
	prompt_text += "If you consider that the user is not doing well lower the difficulty level, if they are too strong highten the difficulty level." 
	prompt_text += "the last field is the reason why you adjusted the difficulty a certain way, please follow a JSON format {\"help\":\"\", \"difficulty_level\":0, \"reason\":\"\"}"
	
	
	#var image_path = #"res://screenshots/screenshot-1741159543.png"  # Or "user://your_image.png", etc.
	
	var base64_image = ""
	if FileAccess.file_exists(SignalBus.last_screenshot):
		var image = Image.load_from_file(SignalBus.last_screenshot)
	# 1. Encode the image to base64.
		var image_bytes = image.save_png_to_buffer() # Use PNG for best results with Gemini
		base64_image = Marshalls.raw_to_base64(image_bytes)
	
	#signal for analytics
	#TODO: rework the signal
	#SignalBus.gemini_help_requested_details.emit(prompt_text, str(SignalBus.session_id)+ "_screenshot_"+str(SignalBus.last_screenshot_timestamp)+".png")
	
	#call gemini
	call_gemini_with_prompt_and_image(base64_image, prompt_text)

###call gemini with prompt & an image
func call_gemini_with_prompt_and_image(base64_image:String, prompt_text: String):
	var http_request  = HTTPRequest.new()
	add_child(http_request)
	# 2. Construct the request body (JSON).
	var request_data = {
		"contents": [
			{
				"parts": [
					{
						"text": prompt_text
					},
					{
						"inline_data": {
							"mime_type": "image/png",  # Important: Set the correct MIME type.
							"data": base64_image
						}
					}
				]
			}
		]
	}
	var json_string = JSON.stringify(request_data)

	# 3. Create an HTTPRequest node.

	http_request.request_completed.connect(_on_call_gemini_with_prompt_and_image_completed)

	# 4. Set headers.
	var headers = [
		"Content-Type: application/json"
		]

	# 5. Make the request.
	
	if is_gemini_error: #switch between models if we have some errors
		current_gemini_model = 1 - current_gemini_model
	
	var error = http_request.request(GEMINI_PRO_URL[current_gemini_model], headers, HTTPClient.METHOD_POST, json_string)
	if error != OK:
		printerr("Gemini help: HTTP request failed: ", error)
		http_request.queue_free()  # Clean up on error
		return

	#print("Sending request to Gemini...")
### help from gemini
func _on_call_gemini_with_prompt_and_image_completed(_result, response_code, _headers, body):
	# Remove the HTTPRequest node now that we're done with it.
	if response_code == 200:
		# Success! Parse the JSON response.
		var response_json = JSON.parse_string(body.get_string_from_utf8())

		if response_json and response_json.has("candidates") and response_json["candidates"].size() > 0:
			if response_json["candidates"][0].has("content") and response_json["candidates"][0]["content"].has("parts"):
				var generated_text = response_json["candidates"][0]["content"]["parts"][0]["text"]
				#print(generated_text)
				var parsedJSON = JSON.parse_string(generated_text.replace("```json", "").replace("```", ""))  
				if parsedJSON.has("help"):
					var  r = ""
					if parsedJSON.has("reason"):
						r = parsedJSON['reason']
					SignalBus.gemini_help_received.emit(parsedJSON["help"]+" \n Difficulty Level: "+str(r))
					
					if parsedJSON.has("difficulty_level"):
						#set difficulty
						SignalBus.prev_game_difficulty = SignalBus.game_difficulty	
						if parsedJSON["difficulty_level"] == 2:
								#print("hard")
								SignalBus.game_difficulty = SignalBus.HARD
						elif parsedJSON["difficulty_level"] == 1:
								#print("med")
								SignalBus.game_difficulty = SignalBus.MEDIUM
						else:		
								#print("easy")
								SignalBus.game_difficulty = SignalBus.EASY
						SignalBus.gemini_difficulty_adjusted.emit(parsedJSON["difficulty_level"], parsedJSON['reason'])
						is_gemini_error = false
					else:
						is_gemini_error = true
						printerr("Gemini help:  Missing difficulty level):", response_json)
				else:
						is_gemini_error = true
						printerr("Gemini help: Missing help & reason ):", response_json)
						
			else:
				is_gemini_error = true
				printerr("Gemini help: Unexpected response format (no content/parts):", response_json)
		else: 
			is_gemini_error = true
			printerr("Gemini help: Unexpected response format (no candidates):", response_json)

	else:
		printerr("Gemini help: HTTP request failed with code ", response_code)
		printerr("Response body:\n", body.get_string_from_utf8())  # Print the raw response for debugging.
#######

####### generate directly story background image with imagen3
func call_imagen3_generate_image(prompt_text: String):
	# 1. Construct the request body.
	var request_data = {
		"contents": [
			{
				"parts": [
					{
						"text": prompt_text
					}
				]
			}
		]
	}
	#imagen3 format
	request_data = {
		"instances": [
			{
				"prompt": prompt_text
			}
			],
			"parameters": {
			"sampleCount": 1,
			"aspectRatio": "1:1",
			"negativePrompt": "",
			"enhancePrompt": false,
			"personGeneration": "allow_adult",
			"safetySetting": "block_few",
			"addWatermark": true,
			"includeRaiReason": true,
			"language": "auto"
			}	
	}
	
	
	var json_string = JSON.stringify(request_data)

	# 2. Create an HTTPRequest node.
	var http_request = HTTPRequest.new()
	add_child(http_request)
	http_request.request_completed.connect( _on_background_story_image_completed)

	# 3. Set headers.
	var headers = [
		"Content-Type: application/json",
		"Authorization: Bearer "+ gcp_token
	]

	# 4. Make the request.
	var error = http_request.request(IMAGEN3_URL, headers, HTTPClient.METHOD_POST, json_string)
	if error != OK:
		printerr("HTTP request failed: ", error)
		http_request.queue_free()
		return

	print("Sending request to Gemini...")

#receive results from Gemini
# TODO go through the API Bridge using python
func _on_background_story_image_completed(_result, response_code, _headers, body):
	if response_code == 200:
		var response_json = JSON.parse_string(body.get_string_from_utf8())
		if response_json and response_json.has("predictions") and response_json["predictions"].size() > 0:
			if response_json["predictions"][0].has("bytesBase64Encoded"):
						# Decode the base64 image data.
						var base64_image = response_json["predictions"][0]["bytesBase64Encoded"]
						SignalBus.gemini_backstory_image = base64_image
						SignalBus.gemini_backstory_image_received.emit(base64_image)
			else:
				printerr("Unexpected response format (no predictions content):", response_json)
		else:
			printerr("Unexpected response format (no predictions):", response_json)
	else:
		printerr("HTTP request failed with code ", response_code)
		printerr("Response body:\n", body.get_string_from_utf8())
#######
