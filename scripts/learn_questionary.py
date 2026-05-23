"""learn questionary"""

from pathlib import Path

import inquirer
import questionary


def main():
    """main, natch"""

    # questions = [
    #     inquirer.Text('report_id', message="Enter the report ID"),
    # ]
    # answers = inquirer.prompt(questions)
    # report_id = answers.get('report_id')

    # print(report_id)
    # if not report_id:
    #     print('You need to supply a report ID or create a new report')

    # report_id = questionary.text(message='Enter the report ID').ask()

    # print(report_id)
    # if not report_id:
    #     print('You need to supply a report ID or create a new report')

    questions = [
        inquirer.Text('outputs_dir', message="Path to the directory containing output files"),
    ]
    answers = inquirer.prompt(questions)
    outputs_dir = Path(answers['outputs_dir'].strip('"'))
    print(outputs_dir)
    print(isinstance(outputs_dir, Path))
    if not outputs_dir.is_dir():
        print('You need to supply a valid path to the directory containing output files')

    outputs_dir = questionary.path(message="Path to the directory containing output files", only_directories=True).ask()
    print(isinstance(outputs_dir, Path))
    print("now I strip")
    outputs_dir = Path(outputs_dir.strip('"'))
    print(outputs_dir)
    print(isinstance(outputs_dir, Path))
    if not outputs_dir.is_dir():
        print('You need to supply a valid path to the directory containing output files')


if __name__ == "__main__":
    main()
