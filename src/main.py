from src.constants import db_path, external_funds_folder
from src.dispatcher import run_tasks
from src.setup import init


def main():
    db = init(external_funds_folder=external_funds_folder, db_path=db_path)
    run_tasks(db)

if __name__ == "__main__":
    main()
