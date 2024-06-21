-- PART 3 TASK 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде

CREATE OR REPLACE FUNCTION fnc_task_3_1()
    RETURNS TABLE(Peer1 VARCHAR, Peer2 VARCHAR, Points BIGINT)
AS $$
BEGIN
    RETURN QUERY
        SELECT tp1.peer1, tp1.peer2, tp1.points_amount - COALESCE((SELECT tp2.points_amount FROM transferred_points tp2 WHERE tp2.peer1 = tp1.peer2 AND tp2.peer2 = tp1.peer1), 0)
        FROM transferred_points tp1
        ORDER BY 3;
END;
$$ 
LANGUAGE plpgsql;

SELECT * FROM fnc_task_3_1();
-- NEED FOR TEST TASK 3 EXERCIZE 1
SELECT * FROM transferred_points WHERE peer1 = 'gdlzzcthpd' AND peer2 = 'iosfiypdje';
SELECT * FROM transferred_points WHERE peer1 = 'iosfiypdje' AND peer2 = 'gdlzzcthpd';

-- PART 3 TASK 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP

CREATE FUNCTION fnc_succes_task()
    RETURNS TABLE
            (
                nickname  VARCHAR,
                task      VARCHAR,
                xp_amount BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT all_d.peer, all_d.task, all_d.xp_amount
        FROM (SELECT checks.peer, checks.task, xp.xp_amount
              FROM checks
                       JOIN p2p ON checks.id = p2p.check_value
                       JOIN verter ON checks.id = verter.check_value
                       JOIN xp ON checks.id = xp.check_value
              WHERE verter.state = 'Success'
                AND p2p.state = 'Success') all_d
        ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM fnc_succes_task();


-- PART 3 TASK 3)  Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
CREATE FUNCTION fnc_no_exit_peers(date_ DATE)
    RETURNS TABLE
            (
                peer VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT DISTINCT t1.peer
        FROM time_tracking
                 JOIN (SELECT time_tracking.peer, date, SUM(state) = 1 AS count_
                       FROM time_tracking
                       GROUP BY time_tracking.peer, date) AS t1 ON t1.peer = time_tracking.peer
        WHERE t1.count_ IS TRUE
          AND t1.date = date_;
END;
$$ LANGUAGE plpgsql;


SELECT *
FROM fnc_no_exit_peers('2022-02-20');
-- SELECT * FROM time_tracking WHERE peer = 'nbptgjyamh' --TRUE
-- SELECT * FROM time_tracking WHERE peer = 'aytzcyxgof' --TRUE
-- SELECT * FROM time_tracking WHERE peer = 'aahvshcdxk' --TRUE
-- SELECT * FROM time_tracking WHERE peer = 'abpfymnduw' --TRUE
-- SELECT * FROM time_tracking WHERE peer = 'oihhthxcgt' --true

-- PART 3 TASK 4) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints

CREATE OR REPLACE FUNCTION fnc_changed_points()
    RETURNS TABLE
            (
                checking_peer VARCHAR,
                points_amount NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT p1.peer1, p1.s1 - p2.s2 AS PointsChange
        FROM (SELECT transferred_points.peer1, SUM(transferred_points.points_amount) AS s1
              FROM transferred_points
              GROUP BY 1
             ) p1
                 JOIN peers p ON p.nickname = p1.peer1
                 JOIN (SELECT peer2, SUM(transferred_points.points_amount) s2
                       FROM transferred_points
                       GROUP BY 1) AS p2 ON p1.peer1 = p2.peer2
        ORDER BY 2;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM fnc_changed_points();

-- PART 3 TASK 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3

CREATE OR REPLACE FUNCTION fnc_changed_points_from_first_task()
    RETURNS TABLE
            (
                checking_peer VARCHAR,
                points_amount NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT p1.Peer1, p1.s1 - p2.s2 AS PointsChange
        FROM (SELECT fnc_task_3_1.Peer1, SUM(fnc_task_3_1.Points) AS s1
              FROM fnc_task_3_1()
              GROUP BY fnc_task_3_1.Peer1
             ) p1
                 JOIN peers p ON p.nickname = p1.Peer1
                 JOIN (SELECT fnc_task_3_1.Peer2, SUM(fnc_task_3_1.Points) AS s2
                       FROM fnc_task_3_1()
                       GROUP BY fnc_task_3_1.Peer2
        ) AS p2 ON p1.Peer1 = p2.Peer2
        ORDER BY 2;
END;
$$ LANGUAGE plpgsql;


SELECT *
FROM fnc_changed_points_from_first_task();

-- PART 3 TASK 6) Определить самое часто проверяемое задание за каждый день
CREATE OR REPLACE PROCEDURE often_checked_task_of_day()
    LANGUAGE plpgsql
AS
$$
DECLARE
    it_date                   DATE;
    max_checking_count_of_day INT;
BEGIN
    -- создаем временную таблицу со всеми датами и тасками
    CREATE TEMPORARY TABLE all_date_and_tasks AS (SELECT date AS Day, task AS Task FROM checks GROUP BY 2, 1 ORDER BY 1);
    -- вытаскиваем каждый день отдельно для итерации
    FOR it_date IN (SELECT date FROM checks GROUP BY 1 ORDER BY 1)
        LOOP
            -- вытаскиваем максимальное количество проверок за it_date день для сравнения
            max_checking_count_of_day := (SELECT MAX(max_count.count)
                                          FROM (SELECT date, task, COUNT(task)
                                                FROM checks
                                                WHERE date = it_date
                                                GROUP BY 1, 2
                                                ORDER BY 3 DESC) AS max_count);
            IF (max_checking_count_of_day > 1) THEN
                DELETE
                FROM all_date_and_tasks
                WHERE (Day, Task) IN (SELECT date, task
                                      FROM checks
                                      WHERE date = it_date
                                      GROUP BY 1, 2
                                      HAVING COUNT(task) < max_checking_count_of_day);
            END IF;
        END LOOP;
END;
$$;

CALL often_checked_task_of_day();
SELECT *
FROM all_date_and_tasks;


-- PART 3 TASK 7) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
CREATE OR REPLACE PROCEDURE task_3_7(task_block VARCHAR(3))
    LANGUAGE plpgsql
AS
$$
DECLARE
    nick           VARCHAR(255);
    last_date_chek DATE;
BEGIN
    -- пустая таблица для результата
    CREATE TEMPORARY TABLE table_task_3_7
    (
        Peer VARCHAR(255),
        Day  DATE
    );
    -- таблица со всеми проектами введенного блока
    CREATE TEMPORARY TABLE table_task_block AS (SELECT ROW_NUMBER() OVER () AS id, title
                                                FROM tasks
                                                WHERE title LIKE task_block || '_');
    IF (NOT EXISTS(SELECT * FROM table_task_block)) THEN
        RAISE EXCEPTION 'Input block is not found!';
    END IF;
    -- цикл по каждому пиру
    FOR nick IN (SELECT nickname FROM peers)
        LOOP
            IF (NOT EXISTS(SELECT title
                           FROM table_task_block
                           EXCEPT
                           SELECT c.task
                           FROM checks c
                                    JOIN p2p ON c.id = p2p.check_value AND p2p.state = 'Success'
                           WHERE c.peer = nick
                             AND task LIKE task_block || '_')) THEN
                -- поиск последней даты последнего проекта блока
                SELECT INTO last_date_chek MAX(c.date)
                FROM checks c
                         JOIN (SELECT * FROM table_task_block ORDER BY 1 DESC LIMIT 1) AS tt ON c.task = tt.title
                         JOIN p2p ON c.id = p2p.check_value AND p2p.state = 'Success'
                WHERE c.peer = nick;

                INSERT INTO table_task_3_7 VALUES (nick, last_date_chek);
            END IF;
        END LOOP;
END;
$$;

CALL task_3_7('SQL');
SELECT *
FROM table_task_3_7;

-- PART 3 TASK 8) пределить, к какому пиру стоит идти на проверку каждому обучающемуся
-- Для заполнения таблицы так как нету подходящих рекомендаций
-- INSERT INTO recommendations VALUES ((SELECT MAX(id) + 1 FROM recommendations), 'iodskxuoka', 'mvazvelhwy');
-- INSERT INTO recommendations VALUES ((SELECT MAX(id) + 1 FROM recommendations), 'hsygfqpicc', 'ecqnbfpiue');
-- INSERT INTO recommendations VALUES ((SELECT MAX(id) + 1 FROM recommendations), 'ciqvjyxubt', 'krwjayqexx');
-- INSERT INTO recommendations VALUES ((SELECT MAX(id) + 1 FROM recommendations), 'xyzddvrfnu', 'rpwqhaxkeu');
-- INSERT INTO recommendations VALUES ((SELECT MAX(id) + 1 FROM recommendations), 'drorrmvnmm', 'xdurlvroay');
-- INSERT INTO recommendations VALUES ((SELECT MAX(id) + 1 FROM recommendations), 'bwohnyapta', 'thyrtwnsgs');
-- INSERT INTO recommendations VALUES ((SELECT MAX(id) + 1 FROM recommendations), 'onwxgadnyu', 'fjztnkwlhg');
-- INSERT INTO recommendations VALUES ((SELECT MAX(id) + 1 FROM recommendations), 'yumejwomtb', 'prbedzugjq');
-- INSERT INTO recommendations VALUES ((SELECT MAX(id) + 1 FROM recommendations), 'wwmtmdxndn', 'ubvvcurpkw');
-- INSERT INTO recommendations VALUES ((SELECT MAX(id) + 1 FROM recommendations), 'aazjbjbtfe', 'mvazvelhwy');
-- INSERT INTO recommendations VALUES ((SELECT MAX(id) + 1 FROM recommendations), 'qalstoqnlb', 'aazjbjbtfe');

CREATE OR REPLACE FUNCTION task_3_8()
    RETURNS TABLE (Peer VARCHAR, RecommendedPeer VARCHAR)
AS $$
DECLARE
    nick                 VARCHAR;
    recommendations_nick VARCHAR;
BEGIN
    -- создаем пустую результирующую таблицу
    CREATE TEMPORARY TABLE table_task_3_8(Peer VARCHAR(255), RecommendedPeer VARCHAR(255));
    -- итерируемся по каждому пиру
    FOR nick IN (SELECT nickname FROM peers)
        LOOP
            --запрос на нахождения рекомендованного пира из рекомендаций друзей
            SELECT recommended_peer
            INTO recommendations_nick
            FROM (SELECT recommended_peer, COUNT(recommended_peer) AS count
                  FROM recommendations r
                           JOIN (SELECT peer1
                                 FROM friends
                                 WHERE peer2 = nick
                                 UNION
                                 SELECT peer2
                                 FROM friends
                                 WHERE peer1 = nick) AS p1 ON r.peer = p1.peer1
                  GROUP BY 1
                  ORDER BY 2 DESC
                  LIMIT 1) AS nickname;
            -- если друзей нету, то рекомендованный пир выбирается рандомным образом
            IF (recommendations_nick IS NULL) THEN
                SELECT nickname INTO recommendations_nick FROM peers ORDER BY RANDOM() LIMIT 1;
            END IF;
            -- запись в результирующую таблицу
            INSERT INTO table_task_3_8 VALUES (nick, recommendations_nick);
        END LOOP;
    RETURN QUERY SELECT * FROM table_task_3_8;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM task_3_8();


-- PART 3 TASK 9) Определить процент пиров, которые: Приступили только к блоку 1, 2 | Приступили к обоим | Не приступили ни к одному
CREATE OR REPLACE PROCEDURE prc_block(
    IN name_block_1 VARCHAR,
    IN name_block_2 VARCHAR,
    OUT StartedBlock1 VARCHAR,
    OUT StartedBlock2 VARCHAR,
    OUT StartedBothBlocks VARCHAR,
    OUT DidntStartAnyBlock VARCHAR) AS
$$
BEGIN
    WITH all_pers AS (
        SELECT COUNT(*) AS count
        FROM peers
    ),
         StartedBlock1 AS (
             SELECT nickname
             FROM peers
             WHERE nickname IN (SELECT checks.peer
                                FROM checks
                                WHERE task LIKE name_block_1 || '%')
               AND nickname NOT IN (SELECT checks.peer
                                    FROM checks
                                    WHERE task LIKE name_block_2 || '%')
         ),
         StartedBlock2 AS (
             SELECT nickname
             FROM peers
             WHERE nickname IN (SELECT checks.peer
                                FROM checks
                                WHERE task LIKE name_block_2 || '%')
               AND nickname NOT IN (SELECT checks.peer
                                    FROM checks
                                    WHERE task LIKE name_block_1 || '%')
         ),
         StartedBothBlocks AS (
             SELECT nickname
             FROM peers
             WHERE nickname IN (SELECT checks.peer
                                FROM checks
                                WHERE task LIKE name_block_1 || '%')
               AND nickname IN (SELECT checks.peer
                                FROM checks
                                WHERE task LIKE name_block_2 || '%')
         ),
         DidntStartAnyBlock AS (
             SELECT nickname
             FROM peers
             WHERE nickname NOT IN (SELECT checks.peer
                                    FROM checks
                                    WHERE task LIKE name_block_2 || '%')
               AND nickname NOT IN (SELECT checks.peer
                                    FROM checks
                                    WHERE task LIKE name_block_1 || '%')
         )
    SELECT ROUND((SELECT COUNT(sb1.nickname) FROM StartedBlock1 sb1)::NUMERIC / (ap.count), 2) * 100    AS startedBl_1,
           ROUND((SELECT COUNT(sb2.nickname) FROM StartedBlock2 sb2)::NUMERIC / (ap.count), 2) * 100    AS startedBl_2,
           ROUND((SELECT COUNT(sb.nickname) FROM StartedBothBlocks sb)::NUMERIC / (ap.count), 2) * 100  AS startedBoth,
           ROUND((SELECT COUNT(ds.nickname) FROM DidntStartAnyBlock ds)::NUMERIC / (ap.count), 2) * 100 AS dontstart
    INTO StartedBlock1, StartedBlock2, StartedBothBlocks, DidntStartAnyBlock
    FROM all_pers ap;
END;
$$
    LANGUAGE plpgsql;

CALL prc_block('CPP', 'SQL', StartedBlock1 := NULL, StartedBlock2 := NULL, StartedBothBlocks := NULL,
               DidntStartAnyBlock := NULL);

-- PART 3 TASK 10) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
CREATE OR REPLACE FUNCTION fnc_precent_birthday()
    RETURNS TABLE
            (
                SuccessfulChecks   NUMERIC,
                UnsuccessfulChecks NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH SuccessfulChecks AS (
            SELECT COUNT(*) AS n1
            FROM peers p
                     JOIN (SELECT id, peer, date
                           FROM checks) AS ch ON ch.peer = p.nickname
                     JOIN (SELECT check_value, state
                           FROM p2p
                           WHERE state = 'Success') AS p2ps ON p2ps.check_value = ch.id
            WHERE TO_CHAR(p.birthday, 'MM-DD') LIKE TO_CHAR(ch.date, 'MM-DD')
        ),
             UnsuccessfulChecks AS (
                 SELECT COUNT(*) AS n2
                 FROM peers p
                          JOIN (SELECT id, peer, date
                                FROM checks) AS ch ON ch.peer = p.nickname
                          JOIN (SELECT check_value, state
                                FROM p2p
                                WHERE state = 'Failure') AS p2ps ON p2ps.check_value = ch.id
                 WHERE TO_CHAR(p.birthday, 'MM-DD') LIKE TO_CHAR(ch.date, 'MM-DD')
             )

        SELECT (SELECT SuccessfulChecks.n1 FROM SuccessfulChecks)::NUMERIC /
               ((SELECT SuccessfulChecks.n1 FROM SuccessfulChecks) +
                (SELECT UnsuccessfulChecks.n2 FROM UnsuccessfulChecks)) * 100 AS SuccessCh,
               (SELECT UnsuccessfulChecks.n2 FROM UnsuccessfulChecks)::NUMERIC /
               ((SELECT SuccessfulChecks.n1 FROM SuccessfulChecks) +
                (SELECT UnsuccessfulChecks.n2 FROM UnsuccessfulChecks)) * 100 AS UnsuccessCh
        FROM SuccessfulChecks,
             UnsuccessfulChecks;
END;
$$ LANGUAGE plpgsql;
--
SELECT *
FROM fnc_precent_birthday();

-- PART 3 TASK 11) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
CREATE OR REPLACE PROCEDURE task_3_11(task1 VARCHAR, task2 VARCHAR, task3 VARCHAR)
AS
$$
BEGIN
    CREATE TEMPORARY TABLE table_task_3_11 AS (SELECT nickname
                                               FROM peers
                                               WHERE nickname IN (SELECT peer
                                                                  FROM checks
                                                                           JOIN (SELECT check_value, state FROM verter WHERE state = 'Success') AS v
                                                                                ON v.check_value = checks.id
                                                                  WHERE task LIKE task1)
                                                 AND nickname IN (SELECT peer
                                                                  FROM checks
                                                                           JOIN (SELECT check_value, state FROM verter WHERE state = 'Success') AS v
                                                                                ON v.check_value = checks.id
                                                                  WHERE task LIKE task2)
                                                 AND nickname IN (SELECT peer
                                                                  FROM checks
                                                                           JOIN (SELECT check_value, state FROM verter WHERE state = 'Failure') AS v
                                                                                ON v.check_value = checks.id
                                                                  WHERE task LIKE task3));
END;
$$ LANGUAGE plpgsql;

CALL task_3_11('CPP3', 'CPP1', 'SQL1');
SELECT *
FROM table_task_3_11;


-- PART 3 TASK 12)  Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
CREATE OR REPLACE FUNCTION fnc_recursive() RETURNS TABLE(title VARCHAR, val INT) AS
$$
BEGIN
    RETURN QUERY
    WITH RECURSIVE task_lust AS (
        SELECT t0.title, 0 AS val
        FROM tasks t0
        WHERE t0.title = 'C1'

        UNION ALL

        SELECT t.title, l.val + 1
        FROM tasks t
                 JOIN task_lust l ON t.parent_task = l.title
    )
    SELECT tl.title, tl.val
    FROM task_lust tl;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_recursive();

-- 13) Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы N идущих подряд успешных проверки
CREATE OR REPLACE PROCEDURE searching_lucky_day(n INT)
    LANGUAGE plpgsql
AS
$$
DECLARE
    it_date         DATE;
    count_lucky_day INT;
    checking_date   RECORD;
BEGIN
    IF (n < 0) THEN
        RAISE EXCEPTION 'Inputs value n is less than 0!';
    ELSE
        -- создаем временную таблицу с датами в которых количество успешно сданных проектов >= n
        CREATE TEMPORARY TABLE all_date AS (SELECT c.date
                                            FROM checks c
                                                     JOIN p2p p ON c.id = p.check_value AND p.state = 'Success'
                                            GROUP BY 1
                                            HAVING COUNT(p.state) >= n
                                            ORDER BY 1);
        FOR it_date IN (SELECT * FROM all_date)
            LOOP
                count_lucky_day := 0;
                -- вытаскиваем все данные по дню it_date
                FOR checking_date IN (SELECT c.date, p.time, p.state, c.task, p.check_value
                                      FROM p2p p
                                               JOIN checks c
                                                    ON c.date = it_date AND p.check_value = c.id AND p.state != 'Start'
                                      ORDER BY 1, 2)
                    LOOP
                        CASE checking_date.state
                            WHEN 'Success' THEN IF (EXISTS(SELECT xp.xp_amount >= (t.max_xp * 0.8)
                                                           FROM XP xp
                                                                    JOIN tasks t ON title = checking_date.task
                                                           WHERE check_value = checking_date.check_value)) THEN
                                count_lucky_day := count_lucky_day + 1;
                            ELSE
                                count_lucky_day := 0;
                            END IF;
                            ELSE count_lucky_day := 0;
                            END CASE;
                    END LOOP;
                IF
                    (count_lucky_day < n)
                THEN
                    DELETE
                    FROM all_date
                    WHERE date = it_date;
                END IF;
            END LOOP;
    END IF;
END;
$$;

CALL searching_lucky_day(2);
SELECT *
FROM all_date;

-- 14) Определить пира с наибольшим количеством XP
CREATE OR REPLACE FUNCTION task_3_14()
    RETURNS TABLE
            (
                nickname  VARCHAR,
                xp_amount NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT p.nickname, SUM(xp.xp_amount) AS XP
        FROM peers p
                 JOIN checks c ON p.nickname = c.peer
                 JOIN p2p ON c.id = p2p.check_value AND p2p.state = 'Success'
                 JOIN xp ON c.id = xp.check_value
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 1;
END;
$$
    LANGUAGE plpgsql;

SELECT *
FROM task_3_14();
DROP FUNCTION task_3_14();

-- 15) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
CREATE OR REPLACE PROCEDURE task_3_15(in_time TIME, n BIGINT)
    LANGUAGE plpgsql
AS
$$
BEGIN
    CREATE TEMPORARY TABLE table_task_3_15 AS (SELECT peer
                                               FROM time_tracking tt
                                               WHERE tt.state = 1
                                                 AND tt.time < in_time
                                               GROUP BY 1
                                               HAVING COUNT(peer) >= n);
END;
$$;

CALL task_3_15('18:00:00', 4);
SELECT *
FROM table_task_3_15;

-- 16) Определить пиров, выходивших за последние N дней из кампуса больше M раз
CREATE OR REPLACE PROCEDURE task_3_16(n INT, m INT)
    LANGUAGE plpgsql
AS
$$
BEGIN
    CREATE TEMPORARY TABLE table_task_3_16 AS (SELECT tt.peer
                                               FROM time_tracking tt
                                               WHERE tt.state = 2
                                                 AND tt.date BETWEEN NOW()::DATE - n AND NOW()::DATE
                                               GROUP BY 1
                                               HAVING COUNT(peer) > m);
END;
$$;

CALL task_3_16(400, 5);
SELECT *
FROM table_task_3_16;

-- PART 3 TASK 17) Определить для каждого месяца процент ранних входов
CREATE OR REPLACE FUNCTION fnc_percent_of_month()
    RETURNS TABLE
            (
                Month        TEXT,
                EarlyEntries NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT t1.Month, ROUND(t2.erly::NUMERIC / t1.count_all_time, 2) * 100 AS EarlyEntries
        FROM (SELECT TO_CHAR(birthday, 'MONTH') AS Month, COUNT(*) AS count_all_time
              FROM peers p
                       JOIN (SELECT peer, date, state, ROW_NUMBER() OVER (PARTITION BY peer) AS p_count_day
                             FROM time_tracking
                             WHERE state = 1
                             GROUP BY 1, 2, 3) AS tr ON tr.peer = p.nickname
              WHERE EXTRACT(MONTH FROM p.birthday) = EXTRACT(MONTH FROM tr.date)
              GROUP BY 1
             ) t1
                 JOIN (SELECT TO_CHAR(birthday, 'MONTH') AS Month, COUNT(*) AS erly
                       FROM peers p
                                JOIN (SELECT peer, date, state, ROW_NUMBER() OVER (PARTITION BY peer) AS p_count_day
                                      FROM time_tracking
                                      WHERE state = 1
                                        AND time BETWEEN '00:00:00'
                                          AND '12:00:00'
                                      GROUP BY 1, 2, 3) AS tr2 ON tr2.peer = p.nickname
                       WHERE EXTRACT(MONTH FROM p.birthday) = EXTRACT(MONTH FROM tr2.date)
                       GROUP BY 1) AS t2 ON t1.Month = t2.Month;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM fnc_percent_of_month();
