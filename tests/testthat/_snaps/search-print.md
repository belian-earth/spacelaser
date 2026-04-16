# print.sl_gedi_search renders header and rows

    Code
      print(make_gedi_search(3L))
    Message
      <sl_gedi_search> | GEDI L2A | 3 granules | (-124.0400, 41.3900) - (-124.0100,
      41.4200)
    Output
      # A tibble: 3 x 5
        id    time_start          time_end            url                     geometry
        <chr> <dttm>              <dttm>              <chr>                   <wk_wkt>
      1 G-01  2022-01-01 01:00:00 2022-01-01 01:01:00 https://example.test/G~ POLYGON~
      2 G-02  2022-01-01 02:00:00 2022-01-01 02:01:00 https://example.test/G~ POLYGON~
      3 G-03  2022-01-01 03:00:00 2022-01-01 03:01:00 https://example.test/G~ POLYGON~

# print.sl_gedi_search truncates after n = 10 with a tail line

    Code
      print(make_gedi_search(12L))
    Message
      <sl_gedi_search> | GEDI L2A | 12 granules | (-124.0400, 41.3900) - (-124.0100,
      41.4200)
    Output
      # A tibble: 10 x 5
         id    time_start          time_end            url                    geometry
         <chr> <dttm>              <dttm>              <chr>                  <wk_wkt>
       1 G-01  2022-01-01 01:00:00 2022-01-01 01:01:00 https://example.test/~ POLYGON~
       2 G-02  2022-01-01 02:00:00 2022-01-01 02:01:00 https://example.test/~ POLYGON~
       3 G-03  2022-01-01 03:00:00 2022-01-01 03:01:00 https://example.test/~ POLYGON~
       4 G-04  2022-01-01 04:00:00 2022-01-01 04:01:00 https://example.test/~ POLYGON~
       5 G-05  2022-01-01 05:00:00 2022-01-01 05:01:00 https://example.test/~ POLYGON~
       6 G-06  2022-01-01 06:00:00 2022-01-01 06:01:00 https://example.test/~ POLYGON~
       7 G-07  2022-01-01 07:00:00 2022-01-01 07:01:00 https://example.test/~ POLYGON~
       8 G-08  2022-01-01 08:00:00 2022-01-01 08:01:00 https://example.test/~ POLYGON~
       9 G-09  2022-01-01 09:00:00 2022-01-01 09:01:00 https://example.test/~ POLYGON~
      10 G-10  2022-01-01 10:00:00 2022-01-01 10:01:00 https://example.test/~ POLYGON~
    Message
      # ... with 2 more granules

# print.sl_gedi_search reports no granules for an empty search

    Code
      print(make_gedi_search(0L))
    Message
      <sl_gedi_search> | GEDI L2A | 0 granules | (-124.0400, 41.3900) - (-124.0100,
      41.4200)
      (no granules)

# print.sl_icesat2_search renders header and rows

    Code
      print(make_icesat2_search(3L))
    Message
      <sl_icesat2_search> | ICESat-2 ATL08 | 3 granules | (-124.0400, 41.3900) -
      (-124.0100, 41.4200)
    Output
      # A tibble: 3 x 5
        id    time_start          time_end            url                     geometry
        <chr> <dttm>              <dttm>              <chr>                   <wk_wkt>
      1 G-01  2022-01-01 01:00:00 2022-01-01 01:01:00 https://example.test/A~ POLYGON~
      2 G-02  2022-01-01 02:00:00 2022-01-01 02:01:00 https://example.test/A~ POLYGON~
      3 G-03  2022-01-01 03:00:00 2022-01-01 03:01:00 https://example.test/A~ POLYGON~

# print.sl_icesat2_search reports no granules for an empty search

    Code
      print(make_icesat2_search(0L))
    Message
      <sl_icesat2_search> | ICESat-2 ATL08 | 0 granules | (-124.0400, 41.3900) -
      (-124.0100, 41.4200)
      (no granules)

