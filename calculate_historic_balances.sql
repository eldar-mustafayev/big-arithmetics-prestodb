with out_amount as(
    select 
        block_date, 
        address, 
        token_address,
        --- recursively combine 10 parts of value into one string
        reduce(sequence(10, 1, -1), '', (s, x) -> s || lpad(cast((sum_list[x] + reduce(slice(sum_list, 1, x-1), 0, (s, x) -> (s + x) / 100000000, s -> s)) % 100000000 as varchar), 8, '0'), s -> regexp_extract(s, '([0]*)(\d+)', 2))
            as value
    from(
        select block_date, from_address as address, token_address, array[
            -- divide value into 10 parts to store 256 bits integer
            sum(sum(cast('0' || reverse(substr(reverse(value), 1, 8)) as bigint))) over(partition by from_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 9, 8)) as bigint))) over(partition by from_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 17, 8)) as bigint))) over(partition by from_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 25, 8)) as bigint))) over(partition by from_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 33, 8)) as bigint))) over(partition by from_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 41, 8)) as bigint))) over(partition by from_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 49, 8)) as bigint))) over(partition by from_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 57, 8)) as bigint))) over(partition by from_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 65, 8)) as bigint))) over(partition by from_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 73, 8)) as bigint))) over(partition by from_address, token_address order by block_date rows unbounded preceding)
        ] as sum_list
        from token_transfers
        where value <> '0'
        group by from_address, token_address, block_date
    )
),
in_amount as(
    select block_date, 
        address, 
        token_address, 
        --- recursively combine 10 parts of value into one string
        reduce(sequence(10, 1, -1), '', (s, x) -> s || lpad(cast((sum_list[x] + reduce(slice(sum_list, 1, x-1), 0, (s, x) -> (s + x) / 100000000, s -> s)) % 100000000 as varchar), 8, '0'), s -> regexp_extract(s, '([0]*)(\d+)', 2)) 
            as value
    from(
        select block_date, to_address as address, token_address, array[
            -- divide value into 10 parts to sum 256 bits integer
            sum(sum(cast('0' || reverse(substr(reverse(value), 1, 8)) as bigint))) over(partition by to_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 9, 8)) as bigint))) over(partition by to_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 17, 8)) as bigint))) over(partition by to_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 25, 8)) as bigint))) over(partition by to_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 33, 8)) as bigint))) over(partition by to_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 41, 8)) as bigint))) over(partition by to_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 49, 8)) as bigint))) over(partition by to_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 57, 8)) as bigint))) over(partition by to_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 65, 8)) as bigint))) over(partition by to_address, token_address order by block_date rows unbounded preceding),
            sum(sum(cast('0' || reverse(substr(reverse(value), 73, 8)) as bigint))) over(partition by to_address, token_address order by block_date rows unbounded preceding)
        ] as sum_list
        from token_transfers
        where value <> '0'
        group by to_address, token_address, block_date
    )
)
    select 
        address,
        token_address,
        -- recursively combine 10 parts of value into one string
        reduce(sequence(10, 1, -1), '', (s, x) -> s || lpad(cast((100000000 + subtration_list[x] - reduce(slice(subtration_list, 1, x-1), 0, (s, x) -> cast((x - s) < 0 as int), s -> s)) % 100000000 as varchar), 8, '0'), s -> regexp_extract(s, '([0]*)(\d+)', 2))
            as value,
        date(block_date) as block_date
    from(
        select address, token_address, array[
            -- divide value into 10 parts to subtract 256 bits integer
            cast('0' || reverse(substr(reverse(in_value), 1, 8)) as bigint) - cast('0' || reverse(substr(reverse(out_value), 1, 8)) as bigint),
            cast('0' || reverse(substr(reverse(in_value), 9, 8)) as bigint) - cast('0' || reverse(substr(reverse(out_value), 9, 8)) as bigint),
            cast('0' || reverse(substr(reverse(in_value), 17, 8)) as bigint) - cast('0' || reverse(substr(reverse(out_value), 17, 8)) as bigint),
            cast('0' || reverse(substr(reverse(in_value), 25, 8)) as bigint) - cast('0' || reverse(substr(reverse(out_value), 25, 8)) as bigint),
            cast('0' || reverse(substr(reverse(in_value), 33, 8)) as bigint) - cast('0' || reverse(substr(reverse(out_value), 33, 8)) as bigint),
            cast('0' || reverse(substr(reverse(in_value), 41, 8)) as bigint) - cast('0' || reverse(substr(reverse(out_value), 41, 8)) as bigint),
            cast('0' || reverse(substr(reverse(in_value), 49, 8)) as bigint) - cast('0' || reverse(substr(reverse(out_value), 49, 8)) as bigint),
            cast('0' || reverse(substr(reverse(in_value), 57, 8)) as bigint) - cast('0' || reverse(substr(reverse(out_value), 57, 8)) as bigint),
            cast('0' || reverse(substr(reverse(in_value), 65, 8)) as bigint) - cast('0' || reverse(substr(reverse(out_value), 65, 8)) as bigint),
            cast('0' || reverse(substr(reverse(in_value), 83, 8)) as bigint) - cast('0' || reverse(substr(reverse(out_value), 83, 8)) as bigint)
        ] as subtration_list,
         -- generate missing dates between two balances
        case when in_value = out_value then
            array[block_date]
        else
            sequence(
                block_date,
                lead(block_date - interval '1' day, 1, max_block_date) over(partition by address, token_address order by block_date),
                interval '1' day
            )
        end as null_dates,
        in_value,
        out_value,
        from (
            select
                address,
                token_address,
                block_date,
                coalesce(ain.value, lag(ain.value, 1, '0') ignore nulls over (partition by address, token_address order by block_date)) 
                    as in_value,
                coalesce(aout.value, lag(aout.value, 1, '0') ignore nulls over (partition by address, token_address order by block_date)) 
                    as out_value,
                max(block_date) over(partition by token_address) as max_block_date
            from in_amount ain
            full join out_amount aout using (address, token_address, block_date)
        )
    )
    -- fill missing dates with previous balance
    cross join unnest(null_dates) as t (block_date)
    order by address, token_address, block_date desc