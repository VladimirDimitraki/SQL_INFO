--Создать хранимую процедуру, которая, не уничтожая базу данных,
-- уничтожает все те таблицы текущей базы данных,
-- имена которых начинаются с фразы 'TableName'.

CREATE TABLE IF NOT EXISTS TableName(
    id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS TableName_name(
    id BIGINT PRIMARY KEY
);


CREATE TABLE IF NOT EXISTS bleName_name(
    id BIGINT PRIMARY KEY,
    name VARCHAR(100),
    age INTEGER
);

CREATE OR REPLACE PROCEDURE delete_tables()
LANGUAGE plpgsql
AS $$
DECLARE
    ver_table_name VARCHAR(255);
BEGIN
    FOR ver_table_name IN (SELECT table_name FROM information_schema.tables WHERE table_name ILIKE 'tablename%')
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || ver_table_name || ' CASCADE';
    END LOOP;
END;
$$;

CALL delete_tables();


-- 2) Создать хранимую процедуру с выходным параметром,
-- которая выводит список имен и параметров всех скалярных SQL функций пользователя в
-- текущей базе данных. Имена функций без параметров не выводить.
-- Имена и список параметров должны выводиться в одну строку.
-- Выходной параметр возвращает количество найденных функций.

CREATE OR REPLACE PROCEDURE scalar_output(OUT count_fnc INT)
LANGUAGE plpgsql
AS $$
DECLARE
    fnc_name VARCHAR(255);
BEGIN
    count_fnc := 0;
    FOR fnc_name IN
        SELECT routine_name
        FROM information_schema.routines
        WHERE routine_type = 'FUNCTION' AND data_type != 'void' AND specific_schema = 'public'
    LOOP
        IF fnc_name IS NOT NULL THEN
            RAISE NOTICE 'Function: %', fnc_name;
            count_fnc := count_fnc + 1;
        END IF;
    END LOOP;
END;
$$;


DO $$
DECLARE
  count_fnc INT;
BEGIN
  CALL scalar_output(count_fnc);
  RAISE NOTICE 'кол-во функций с параметром: %', count_fnc;
END $$;

-- 3) Создать хранимую процедуру с выходным параметром,
-- которая уничтожает все SQL DML триггеры в текущей базе данных.
-- Выходной параметр возвращает количество уничтоженных триггеров.


CREATE OR REPLACE PROCEDURE delete_all_triggers(OUT count_trg INT)
LANGUAGE plpgsql
AS $$
DECLARE
    table_record RECORD;
    trigger_record RECORD;
BEGIN
    count_trg := 0;
    FOR table_record IN (
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        )
    LOOP
        FOR trigger_record IN (
            SELECT tgname
            FROM pg_trigger
            WHERE tgrelid = table_record.table_name::regclass
        )
        LOOP
            EXECUTE 'DROP TRIGGER IF EXISTS ' || trigger_record.tgname || ' ON ' || table_record.table_name;
            count_trg := count_trg + 1;
        END LOOP;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION check_tr()
RETURNS TRIGGER
AS $$
BEGIN
    IF NEW.age < 18 THEN
    END IF;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER check_age_trigger
BEFORE INSERT ON bleName_name
FOR EACH ROW
EXECUTE FUNCTION check_tr();


DO $$
DECLARE
    result_count INT;
BEGIN
    CALL delete_all_triggers(result_count);
    RAISE NOTICE 'Удалено триггеров: %', result_count;
END;
$$;


-- 4) Создать хранимую процедуру с входным параметром,
-- которая выводит имена и описания типа объектов (только хранимых процедур и скалярных функций)
-- , в тексте которых на языке SQL встречается строка,
-- задаваемая параметром процедуры.

CREATE OR REPLACE PROCEDURE find_objects_with_string(IN search_string TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    object_name VARCHAR(255);
    object_type VARCHAR(255);
    object_definition TEXT;
BEGIN
    FOR object_name, object_type, object_definition IN (
        SELECT proname,
            CASE WHEN prokind = 'p'
            THEN 'PROCEDURE'
            ELSE 'FUNCTION'
            END,
            pg_get_functiondef(oid) AS definition
        FROM pg_proc
        WHERE prokind IN ('p', 'f')
    )
    LOOP
        IF POSITION(search_string IN object_definition) > 0 THEN
            RAISE NOTICE 'Тип: %, Имя: %', object_type, object_name;
        END IF;
    END LOOP;
END;
$$;


CALL find_objects_with_string('delete_tables');