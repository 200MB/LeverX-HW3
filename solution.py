import os
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector
from dataclasses import dataclass
import xml.etree.ElementTree as ET
import json
from abc import ABC, abstractmethod


@dataclass
class Student:
    id: int
    name: str
    room_id: int
    birthday: datetime.date
    sex: str


@dataclass
class Room:
    id: int
    name: str


class DatabaseConnection(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def execute(self, query, args=None):
        pass

    @abstractmethod
    def executemany(self, query, args: list[tuple]):
        pass

    @abstractmethod
    def fetchall(self):
        pass


class MySqlConnection(DatabaseConnection):
    def __init__(self, host: str, user: str, password: str):
        self.host = host
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None

    def fetchall(self):
        return self.cursor.fetchall()

    def connect(self):
        self.conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password
        )
        self.cursor = self.conn.cursor()
        return self

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def execute(self, query, args=None):
        if args:
            self.cursor.execute(query, args)
        else:
            self.cursor.execute(query)

    def executemany(self, query, args: list[tuple]):
        try:
            self.cursor.executemany(query, args)
            self.conn.commit()
        except mysql.connector.Error as e:
            self.conn.rollback()
            raise

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class DataLoader(ABC):
    """
    Abstract class serving as an interface for all data loading mechanisms
    """

    @abstractmethod
    def load(
            self, students_path: str, rooms_path: str
    ) -> tuple[list[Student], list[Room]]:
        pass


class JsonDataLoader(DataLoader):
    def load(
            self, students_path: str, rooms_path: str
    ) -> tuple[list[Student], list[Room]]:
        def load_json(path):
            with open(path) as f:
                return json.load(f)

        students_json = load_json(students_path)
        rooms_json = load_json(rooms_path)

        students = [Student(s["id"],
                            s["name"],
                            s["room"],
                            datetime.strptime(s["birthday"], '%Y-%m-%dT%H:%M:%S.%f').date(),
                            s["sex"]) for s in students_json]
        rooms = [Room(r["id"], r["name"]) for r in rooms_json]

        return students, rooms


class XmlDataLoader(DataLoader):
    def load(
            self, students_path: str, rooms_path: str
    ) -> tuple[list[Student], list[Room]]:
        def parse_students(xml_path):
            try:
                tree = ET.parse(xml_path)
                students = []
                for student_el in tree.getroot():
                    student = Student(
                        id=int(student_el.find("id").text),
                        name=student_el.find("name").text,
                        room_id=int(student_el.find("room").text),
                        birthday=student_el.find("birthday").text,
                        sex=student_el.find("sex").text,
                    )
                    students.append(student)
                return students
            except ET.ParseError:
                raise ValueError(f"Error parsing XML file: {xml_path}")

        def parse_rooms(xml_path):
            try:
                tree = ET.parse(xml_path)
                rooms = []
                for room_el in tree.getroot():
                    room = Room(
                        id=int(room_el.find("id").text), name=room_el.find("name").text
                    )
                    rooms.append(room)
                return rooms
            except ET.ParseError:
                raise ValueError(f"Error parsing XML file: {xml_path}")

        students = parse_students(students_path)
        rooms = parse_rooms(rooms_path)
        return students, rooms


class DatabaseProcessor(ABC):
    @abstractmethod
    def initialize_db(self):
        pass

    @abstractmethod
    def insert_data(self, students: list[Student], rooms: list[Room]):
        pass

    @abstractmethod
    def clear_tables(self):
        pass


class MySqlProcessor(DatabaseProcessor):
    def __init__(self, database: DatabaseConnection, loader: DataLoader, db_name: str):
        self.database_name = db_name if db_name is not None else "myDb"
        self.loader = loader
        self.database = database

    def _create_database(self):
        create_db = f"""CREATE DATABASE IF NOT EXISTS {self.database_name}"""
        self.database.execute(create_db)

    def _use_database(self):
        connect_to_database = f"""USE {self.database_name}"""
        self.database.execute(connect_to_database)

    def _create_tables(self):
        create_room_table = """
                    CREATE TABLE IF NOT EXISTS rooms
                    (
                        id INTEGER NOT NULL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL
                    );
                """
        self.database.execute(create_room_table)

        create_student_table = """
                    CREATE TABLE IF NOT EXISTS students
                    (
                        id INTEGER NOT NULL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        room_id INTEGER NOT NULL,
                        birthday DATETIME,
                        sex CHAR(1),
                        FOREIGN KEY (room_id) REFERENCES rooms (id)
                    );
                """
        self.database.execute(create_student_table)

    def _create_indexes(self):
        self.database.execute("CREATE INDEX idx_students_room_id ON students (room_id)")
        print("Index 'idx_students_room_id' created.")
        self.database.execute("CREATE INDEX idx_students_birthday ON students (birthday)")
        print("Index 'idx_students_birthday' created.")
        self.database.execute("CREATE INDEX idx_students_room_sex ON students (room_id, sex)")
        print("Index 'idx_students_room_sex' created.")

    def initialize_db(self):
        self._create_database()
        self._use_database()
        self._create_tables()
        self._create_indexes()

    def insert_data(self, students: list[Student], rooms: list[Room]):

        insert_room_data = """INSERT INTO rooms (id, name)
                              VALUES (%s, %s)"""
        rooms_data = [(room.id, room.name) for room in rooms]
        self.database.executemany(insert_room_data, rooms_data)

        insert_student_data = """INSERT INTO students (id, name, room_id, birthday, sex)
                                 VALUES (%s, %s, %s, %s, %s)"""
        students_data = [(student.id, student.name, student.room_id,
                          student.birthday, student.sex) for student in students]
        self.database.executemany(insert_student_data, students_data)

    def clear_tables(self):
        print("Clearing tables")
        self.database.execute("DELETE FROM students")
        self.database.execute("DELETE FROM rooms")

    def _print_student_counts(self):
        query = """SELECT r.name      AS room_name,
                         COUNT(s.id) AS student_count
                  FROM rooms AS r
                           JOIN students AS s
                                ON r.id = s.room_id
                  GROUP BY r.name
                  ORDER BY student_count DESC;"""
        self.database.execute(query)
        results = self.database.fetchall()
        print("Rooms by student count (descending):")
        for room_name, student_count in results:
            print(f"  - Room '{room_name}': {student_count} students")

    def _print_smallest_avg_age(self):
        query = """SELECT r.name                                          AS room_name,
                          AVG(TIMESTAMPDIFF(YEAR, s.birthday, CURDATE())) AS average_age
                   FROM rooms AS r
                            JOIN students AS s
                                 ON r.id = s.room_id
                   GROUP BY r.name
                   ORDER BY average_age ASC LIMIT 5;"""
        self.database.execute(query)
        results = self.database.fetchall()
        print("Top 5 rooms with the smallest average age:")
        for room_name, average_age in results:
            print(f"  - Room '{room_name}': Average age {average_age:.2f} years")

    def _print_largest_age_diff(self):
        query = """SELECT r.name                                                AS room_name,
                          TIMESTAMPDIFF(YEAR, MIN(s.birthday), MAX(s.birthday)) AS age_difference
                   FROM rooms AS r
                            JOIN students AS s
                                 ON r.id = s.room_id
                   GROUP BY r.name
                   ORDER BY age_difference DESC LIMIT 5;"""
        self.database.execute(query)
        results = self.database.fetchall()
        print("Top 5 rooms with the largest age difference:")
        for room_name, age_difference in results:
            print(f"  - Room '{room_name}': Age difference of {age_difference} years")

    def _print_mixed_sex_rooms(self):
        query = """SELECT r.name AS room_name
                   FROM rooms AS r
                            JOIN students AS s
                                 ON r.id = s.room_id
                   GROUP BY r.name
                   HAVING COUNT(DISTINCT s.sex) > 1;"""
        self.database.execute(query)
        results = self.database.fetchall()
        print("Rooms with mixed sex students:")
        for room_name in results:
            print(f"  - Room '{room_name[0]}'")

    def retrieve_filtered_data(self):
        self._print_student_counts()
        print("-" * 50)
        self._print_smallest_avg_age()
        print("-" * 50)
        self._print_largest_age_diff()
        print("-" * 50)
        self._print_mixed_sex_rooms()
        print("-" * 50)

    def run(self):
        """Executes the main application logic."""
        try:
            with self.database as db:
                self.initialize_db()
                self.clear_tables()

                students, rooms = self.loader.load("students.json", "rooms.json")
                print(f"Loaded {len(students)} students and {len(rooms)} rooms.")

                self.insert_data(students, rooms)
                print("Inserted data into database.")

                print("Running all filter commands")
                self.retrieve_filtered_data()

        except Exception as e:
            print(f"An unexpected error occurred during application run: {e}")


def main():
    """
    Instantiate a processor and run the instructions.
    """
    load_dotenv()
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    json_loader = JsonDataLoader()
    database = MySqlConnection(host, user, password)
    mysql_processor = MySqlProcessor(database, json_loader, db_name)
    mysql_processor.run()


if __name__ == "__main__":
    main()
