# Incomplete app!

MENU_PROMPT = "\nEnter 'a' to add a song, 'l' to see your songs, 'f' to find a song by title, or 'q' to quit: "
songs = []


# You may want to create a function for this code
title = input("Enter the song title: ")
director = input("Enter the song singer: ")
year = input("Enter the song release year: ")

songs.append({
    'title': title,
    'singer': director,
    'year': year
})


# Create other functions for:
#   - listing songs
#   - finding songs


# And another function here for the user menu
selection = input(MENU_PROMPT)
while selection != 'q':
    if selection == "a":
        pass
    elif selection == "l":
        pass
    elif selection == "f":
        pass
    else:
        print('Unknown command. Please try again.')

    selection = input(MENU_PROMPT)


# Remember to run the user menu function at the end!