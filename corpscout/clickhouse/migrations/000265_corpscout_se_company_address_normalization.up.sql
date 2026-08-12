CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_addresses
    ADD COLUMN IF NOT EXISTS normalized_address String MATERIALIZED if(
        has_address = 0,
        '',
        arrayStringConcat(
            arrayFilter(component -> component != '', [
                replaceRegexpAll(
                    trim(replaceRegexpAll(
                        lowerUTF8(normalizeUTF8NFKC(ifNull(street_address, ''))),
                        '[^\\p{L}\\p{N}]+',
                        ' '
                    )),
                    ' +[0-9]+ +tr$',
                    ''
                ),
                if(
                    replaceRegexpAll(
                        lowerUTF8(normalizeUTF8NFKC(ifNull(postal_code, ''))),
                        '[^\\p{L}\\p{N}]',
                        ''
                    ) = '00000',
                    '',
                    replaceRegexpAll(
                        lowerUTF8(normalizeUTF8NFKC(ifNull(postal_code, ''))),
                        '[^\\p{L}\\p{N}]',
                        ''
                    )
                ),
                if(
                    trim(replaceRegexpAll(
                        lowerUTF8(normalizeUTF8NFKC(ifNull(post_town, ''))),
                        '[^\\p{L}\\p{N}]+',
                        ' '
                    )) = 'utlandet',
                    '',
                    trim(replaceRegexpAll(
                        lowerUTF8(normalizeUTF8NFKC(ifNull(post_town, ''))),
                        '[^\\p{L}\\p{N}]+',
                        ' '
                    ))
                ),
                lowerUTF8(trim(ifNull(country_code, '')))
            ]),
            '|'
        )
    ) AFTER country_code;

ALTER TABLE corpscout.se_company_addresses_current
    ADD COLUMN IF NOT EXISTS normalized_address String MATERIALIZED if(
        has_address = 0,
        '',
        arrayStringConcat(
            arrayFilter(component -> component != '', [
                replaceRegexpAll(
                    trim(replaceRegexpAll(
                        lowerUTF8(normalizeUTF8NFKC(ifNull(street_address, ''))),
                        '[^\\p{L}\\p{N}]+',
                        ' '
                    )),
                    ' +[0-9]+ +tr$',
                    ''
                ),
                if(
                    replaceRegexpAll(
                        lowerUTF8(normalizeUTF8NFKC(ifNull(postal_code, ''))),
                        '[^\\p{L}\\p{N}]',
                        ''
                    ) = '00000',
                    '',
                    replaceRegexpAll(
                        lowerUTF8(normalizeUTF8NFKC(ifNull(postal_code, ''))),
                        '[^\\p{L}\\p{N}]',
                        ''
                    )
                ),
                if(
                    trim(replaceRegexpAll(
                        lowerUTF8(normalizeUTF8NFKC(ifNull(post_town, ''))),
                        '[^\\p{L}\\p{N}]+',
                        ' '
                    )) = 'utlandet',
                    '',
                    trim(replaceRegexpAll(
                        lowerUTF8(normalizeUTF8NFKC(ifNull(post_town, ''))),
                        '[^\\p{L}\\p{N}]+',
                        ' '
                    ))
                ),
                lowerUTF8(trim(ifNull(country_code, '')))
            ]),
            '|'
        )
    ) AFTER country_code;
