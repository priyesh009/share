
class Library:
    def __init__(self, list, name):
        pass

    def displayBooks(self):
        pass

# Similarly add return book, lend book and add book methods

if __name__ == '__main__':
    student = Library(['Python', 'Think and grom rich'], "Student Library") # you can add more book if you want

    while(True):
        print(f"Welcome to the {student.name} library. Enter your choice to continue")
        print("1. Display Books")
        print("2. Lend a Book")
        print("3. Add a Book")
        print("4. Return a Book")
        user_choice = input()
        if user_choice not in ['1','2','3','4']:
            print("Please enter a valid option")
            continue

        else:
            user_choice = int(user_choice)


#code to get user choice and thier input


