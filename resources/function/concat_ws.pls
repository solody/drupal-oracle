CREATE OR REPLACE FUNCTION CONCAT_WS(p_delim IN VARCHAR2
                                    , p_str1 IN VARCHAR2 DEFAULT NULL
                                    , p_str2 IN VARCHAR2 DEFAULT NULL
                                    , p_str3 IN VARCHAR2 DEFAULT NULL
                                    , p_str4 IN VARCHAR2 DEFAULT NULL
                                    , p_str5 IN VARCHAR2 DEFAULT NULL
                                    , p_str6 IN VARCHAR2 DEFAULT NULL
                                    , p_str7 IN VARCHAR2 DEFAULT NULL
                                    , p_str8 IN VARCHAR2 DEFAULT NULL
                                    , p_str9 IN VARCHAR2 DEFAULT NULL
                                    , p_str10 IN VARCHAR2 DEFAULT NULL
                                    , p_str11 IN VARCHAR2 DEFAULT NULL
                                    , p_str12 IN VARCHAR2 DEFAULT NULL
                                    , p_str13 IN VARCHAR2 DEFAULT NULL
                                    , p_str14 IN VARCHAR2 DEFAULT NULL
                                    , p_str15 IN VARCHAR2 DEFAULT NULL
                                    , p_str16 IN VARCHAR2 DEFAULT NULL
                                    , p_str17 IN VARCHAR2 DEFAULT NULL
                                    , p_str18 IN VARCHAR2 DEFAULT NULL
                                    , p_str19 IN VARCHAR2 DEFAULT NULL
                                    , p_str20 IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2 IS
    TYPE t_str IS VARRAY (20) OF VARCHAR2(4000);
    l_str_list t_str := t_str(p_str1
        , p_str2
        , p_str3
        , p_str4
        , p_str5
        , p_str6
        , p_str7
        , p_str8
        , p_str9
        , p_str10
        , p_str11
        , p_str12
        , p_str13
        , p_str14
        , p_str15
        , p_str16
        , p_str17
        , p_str18
        , p_str19
        , p_str20);
    i          INTEGER;
    l_result   VARCHAR2(4000);
BEGIN
    FOR i IN l_str_list.FIRST .. l_str_list.LAST
        LOOP
            l_result := l_result
                || CASE
                       WHEN l_str_list(i) IS NOT NULL
                           THEN p_delim
                            END
                || CASE
                       WHEN l_str_list(i) = IDENTIFIER.empty_replacer_char()
                           THEN NULL
                       ELSE l_str_list(i)
                            END;
        END LOOP;
    RETURN LTRIM(l_result, p_delim);
END;
