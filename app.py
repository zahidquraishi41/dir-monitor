from database import Database
from typing import List
import os


def cli_menu(title: str, options: List[str]) -> int:
    """Displays a CLI menu with options and prompts the user to choose one.

    Args:
        title (str): The title of the menu.
        options (List[str]): A list of strings representing the menu options.

    Returns:
        int: The index of the selected option.
    """
    while True:
        print()
        print(title)
        for i, option in enumerate(options):
            print(f'{i+1})', option)
        choice = input('Enter your choice: ')
        if choice.isnumeric() and int(choice) > 0 and int(choice) <= len(options):
            return int(choice) - 1
        print('Invalid choice.')


def recorded_menu():
    db = Database()
    recorded = db.recorded()
    db.close()
    if not recorded:
        print('No directory is recorded')
        return None, None
    options = [f'{path} ({time})' for path, time in recorded]
    options.append('Quit')
    choice = cli_menu('Select an recorded directories', options)
    if choice == len(options)-1:
        return None, None
    return recorded, choice


def record():
    path = input('Enter full path of directory: ')

    if not os.path.exists(path):
        print(f"Error: {path} doesn't exists.")
        return
    if os.path.isfile(path):
        print(f'Error: {path} is a file.')
        return
    path = path[:-1] if path.endswith(os.sep) else path

    db = Database()
    db.record(path)
    db.close()
    print('Recorded successfully.')


def changes():
    recorded, choice = recorded_menu()
    if recorded is None:
        return

    db = Database()
    changed = db.changes(*recorded[choice])
    if not any(changed.values()):
        print('No changes have made.')
        return

    for k, v in changed.items():
        if v:
            print(f'--- {k.title()} ---')
            for dir in v:
                print(dir.replace(recorded[choice][0], '..', 1))
            print()

    options = ['Yes', 'No']
    choice = cli_menu('Update changes?', options)
    if choice == 0:
        db.erase(*recorded[choice])
        db.record(recorded[choice][0])
        print('Updated successfully.')
    db.close()


def erase():
    recorded, choice = recorded_menu()
    if recorded is None:
        return

    db = Database()
    db.erase(*recorded[choice])
    db.close()
    print('Removed successfully.')


def main():
    '''Main function that runs the CLI interface for the directory tracking system.'''
    options = ['Record a directory', 'Check for changes',
               'Remove an Recorded directory', 'Quit']
    while True:
        choice = cli_menu('Tracker', options)
        if choice == 0:
            record()
        if choice == 1:
            changes()
        if choice == 2:
            erase()
        if choice == 3:
            break


if __name__ == '__main__':
    main()
