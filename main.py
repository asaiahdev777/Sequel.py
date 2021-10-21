import io
import sqlite3
import sys

global show_progress


def main():
    global show_progress
    show_progress = True
    connection = sqlite3.connect(':memory:')
    connection.isolation_level = None
    cursor = connection.cursor()
    buffer = io.StringIO()
    print('Welcome to Sequel.py. '
          'Type \'exit\' to close this shell at any time.\n'
          'Type \'noprogress\' to disable showing the \'Processing command...\' and \'Done!\' .\n'
          'Type \'showprogress\' to enable showing the \'Processing command...\' and \'Done!\' .\n'
          'Your statements must end in a \';\'')
    sys.stdout.truncate(0)
    start_processing_input(connection, cursor, buffer)


def start_processing_input(connection: sqlite3.Connection, cursor: sqlite3.Cursor, buffer: io.StringIO):
    inputted_text = input()
    buffer.write(inputted_text)
    if len(inputted_text) == 0:
        start_processing_input(connection, cursor, buffer)
    else:
        process_input(inputted_text, connection, cursor, buffer)
    pass


def is_not_empty(text):
    return len(text) != 0


def process_input(text: str, connection: sqlite3.Connection, cursor: sqlite3.Cursor, buffer: io.StringIO):
    global show_progress
    if text.strip().lower() == 'exit':
        prepare_exit_shell()
    elif text.strip().lower() == 'noprogress':
        show_progress = False
        clear_buffer(buffer)
        start_processing_input(connection, cursor, buffer)
    elif text.strip().lower() == 'showprogress':
        show_progress = True
        clear_buffer(buffer)
        start_processing_input(connection, cursor, buffer)
    else:
        commands = list(filter(is_not_empty, text.split(';')))
        for command_verbatim in commands:
            if show_progress:
                print('Processing command...')
            command = command_verbatim.strip().lower()
            try:
                cursor.execute(command + ';')
                if command.startswith('select'):
                    dump_cursor(command_verbatim, cursor)
                elif show_progress:
                    print('Done!')
            except sqlite3.Error as error:
                print(f'Error: {error}')
        start_processing_input(connection, cursor, buffer)


def dump_cursor(command_verbatim, cursor):
    column_tuples = list(cursor.description)
    columns = []
    column_sizes = []
    data = cursor.fetchall()

    for column_tuple in column_tuples:
        column_sizes.append(0)
        columns.append(column_tuple[0])

    total_columns = len(columns)
    total_rows = len(data)
    print(f'Results for {command_verbatim} ({total_rows} rows):')

    for column_number in range(0, total_columns):
        for row_number in range(0, total_rows):
            element = data[row_number][column_number]
            size_of_element = len(str(element))
            if size_of_element > column_sizes[column_number]:
                column_sizes[column_number] = size_of_element
                pass

    for column_number in range(0, total_columns):
        write_item(column_number, column_sizes, columns[column_number])
    for row_number in range(0, total_rows):
        print()
        for column_number in range(0, total_columns):
            write_item(column_number, column_sizes, data[row_number][column_number])
    print()


def write_item(column_number, column_sizes, item):
    size_of_column = column_sizes[column_number] + 2
    size_of_element = len(str(item))
    print(f"{item}{' ' * (size_of_column - size_of_element)}", end='|')


def clear_buffer(buffer):
    buffer.truncate(0)


def prepare_exit_shell():
    commit_changes()
    # close_database()
    exit()


def commit_changes():
    pass


def close_database(database: sqlite3.Connection):
    pass


if __name__ == '__main__':
    main()
