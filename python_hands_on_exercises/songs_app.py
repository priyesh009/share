MENU_PROMPT = "\nEnter 'a' to add a song, 'l' to see your songs, 'f' to find a song by title, or 'q' to quit: "
songs = []


def add_song():
    title = input("Enter the song title: ")
    singer = input("Enter the song singer: ")
    year = input("Enter the song release year: ")

    songs.append({
        'title': title,
        'singer': singer,
        'year': year
    })


def show_songs():
    for song in songs:
        print_song(song)


def print_song(song):
    print(f"Title: {song['title']}")
    print(f"Singer: {song['singer']}")
    print(f"Release year: {song['year']}")


def find_song():
    search_title = input("Enter song title you're looking for: ")

    for song in songs:
        if song["title"] == search_title:
            print_song(song)


user_options = {
    "a": add_song,
    "l": show_songs,
    "f": find_song
}


def menu():
    selection = input(MENU_PROMPT)
    while selection != 'q':
        if selection in user_options:
            selected_function = user_options[selection]
            selected_function()
        else:
            print('Unknown command. Please try again.')

        selection = input(MENU_PROMPT)


menu()