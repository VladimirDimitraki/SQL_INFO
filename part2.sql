-- 1) Написать процедуру добавления P2P проверки
CREATE OR REPLACE PROCEDURE added_p2p_check(in_audit_peer VARCHAR(255), in_checking_peer VARCHAR(255), task_name VARCHAR(255), status check_status, check_time TIME)
LANGUAGE plpgsql
AS $$
BEGIN
  IF (status = 'Start' AND ((SELECT state FROM p2p WHERE id = (SELECT MAX(id) FROM p2p WHERE checking_peer = in_checking_peer)) != 'Start' 
      OR NOT EXISTS (SELECT  state FROM p2p WHERE id = (SELECT MAX(id) FROM p2p WHERE checking_peer = in_checking_peer)))) THEN
    INSERT INTO checks VALUES((SELECT MAX(id) + 1 FROM checks), in_audit_peer, task_name, CURRENT_DATE);
    INSERT INTO P2P VALUES((SELECT MAX(id) FROM P2P) + 1, (SELECT MAX(id) FROM checks), in_checking_peer, 'Start', check_time);
  ELSIF (status != 'Start' AND (SELECT state FROM p2p WHERE id = (SELECT MAX(id) FROM p2p WHERE checking_peer = in_checking_peer)) = 'Start') THEN
    INSERT INTO P2P VALUES((SELECT MAX(id) FROM P2P) + 1, (SELECT MAX(id) FROM checks), in_checking_peer, status, check_time);
  END IF;
END;
$$;
-- 2) Написать процедуру добавления проверки Verter'ом
CREATE OR REPLACE PROCEDURE added_verter_check(in_audit_peer VARCHAR(255), task_name VARCHAR(255), status check_status, check_time TIME)
LANGUAGE plpgsql
AS $$
BEGIN
  IF (status = 'Start' AND EXISTS (SELECT p.time FROM p2p p JOIN checks c ON c.task = task_name 
    AND c.peer = in_audit_peer AND c.id = p.check_value WHERE p.state = 'Success' ORDER BY 1 DESC LIMIT 1) 
    AND (SELECT state FROM verter WHERE id = (SELECT MAX(id) FROM verter)) != 'Start') THEN
      INSERT INTO verter VALUES((SELECT MAX(id) + 1 FROM verter), (SELECT MAX(c.id) FROM checks c WHERE c.task = task_name AND c.peer = audit_peer), status, check_time);
  ELSIF (status != 'Start' AND (SELECT state FROM verter WHERE id = (SELECT MAX(id) FROM verter)) = 'Start') THEN
    INSERT INTO verter VALUES((SELECT MAX(id) + 1 FROM verter), (SELECT MAX(check_value) FROM verter), status, check_time);
  END IF;
END;
$$; 
-- 3) Написать триггер: после добавления записи со статусом "начало" в таблицу P2P, изменить соответствующую запись в таблице TransferredPoints
CREATE OR REPLACE FUNCTION fnc_increment_point() 
RETURNS TRIGGER AS 
$$ 
BEGIN
  IF (NEW.state = 'Start') THEN
    IF (EXISTS (SELECT id FROM transferred_points WHERE peer1 = NEW.checking_peer AND peer2 = (SELECT peer FROM checks WHERE id = NEW.check_value))) THEN
      UPDATE transferred_points SET points_amount = points_amount + 1 WHERE  peer1 = NEW.checking_peer AND peer2 = (SELECT peer FROM checks WHERE id = NEW.check_value);
    ELSE  
      INSERT INTO transferred_points VALUES((SELECT MAX(id)+1 FROM transferred_points), NEW.checking_peer, (SELECT peer FROM checks WHERE id = NEW.check_value), '1');
    END IF;
  END IF;
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER trg_increment_point
AFTER INSERT ON p2p
FOR EACH ROW 
EXECUTE FUNCTION fnc_increment_point(); 

DROP TRIGGER trg_increment_point ON p2p;
DROP FUNCTION fnc_increment_point;
-- 4) Написать триггер: перед добавлением записи в таблицу XP, проверить корректность добавляемой записи
CREATE OR REPLACE FUNCTION fnc_check_added_xp()
RETURNS TRIGGER AS
$$
BEGIN
  IF (NEW.xp_amount > (SELECT max_xp FROM tasks WHERE title = (SELECT task FROM checks WHERE id = NEW.check_value))) THEN
    RAISE EXCEPTION 'Inputs XP is bigger than XP task!';
  ELSIF (NEW.xp_amount < 0) THEN
    RAISE EXCEPTION 'Inputs XP is letter than 0!';
  ELSIF ((SELECT state FROM p2p WHERE id = (SELECT MAX(id) FROM p2p WHERE check_value = NEW.check_value)) != 'Success') THEN
    RAISE EXCEPTION 'Input check_value is not success finished!';
  END IF;
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER trg_check_added_xp
BEFORE INSERT ON XP
FOR EACH ROW
EXECUTE FUNCTION fnc_check_added_xp();

DROP TRIGGER trg_check_added_xp ON XP;
DROP FUNCTION fnc_check_added_xp;
--------------------------------------------------------------------------------------------------------
SELECT * FROM XP;
SELECT * FROM tasks;
SELECT * FROM checks;
SELECT * FROM p2p;

-- For test first task:
CALL added_p2p_check('iosfiypdje', 'gdlzzcthpd', 'AP5', 'Start', '05:38:25');
SELECT * FROM checks WHERE id > 13940; 
SELECT * FROM p2p WHERE check_value > 13940; 
-- For test second task:
CALL added_verter_check('iosfiypdje', 'AP5', 'Start', '02:34:30');
SELECT * FROM verter WHERE check_value > 13940; 
SELECT * FROM p2p WHERE check_value > 13940; 
SELECT * FROM checks WHERE id > 13940; 
-- For test third task:
SELECT * FROM  transferred_points WHERE peer2 = 'iosfiypdje' AND peer1 = 'gdlzzcthpd';
CALL added_p2p_check('iosfiypdje', 'gdlzzcthpd', 'AP5', 'Success', '08:38:25');
SELECT * FROM  transferred_points WHERE peer2 = 'iosfiypdje' AND peer1 = 'gdlzzcthpd';
-- For test fourth task:
INSERT INTO XP VALUES((SELECT MAX(id) + 1 FROM XP), 10330, 100);
SELECT * FROM xp WHERE check_value > 10320;
SELECT * FROM checks WHERE id > 10320; 
SELECT * FROM p2p WHERE check_value > 10320; 

