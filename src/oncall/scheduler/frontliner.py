from . import default
from datetime import datetime, timedelta
from pytz import utc
import operator

ONCALL_USERS = [
    4, # flooey
    13, # gillian
    22, # sven
    23, # jwatzman
    24, # jozef
]

class Scheduler(default.Scheduler):
    def find_least_active_user_id_by_team(self, user_ids, team_id, start_time, role_id, cursor, table_name='event'):
        query = '''
            SELECT `user_id`, MAX(`end`) AS `last_end` FROM `%s`
            WHERE `team_id` = %%s AND `user_id` IN %%s AND `end` <= %%s
            AND `role_id` = %%s
            GROUP BY `user_id`
            ''' % table_name

        # Modification from default scheduler: don't put oncall users on unless
        # two cycles have passed, where two cycles means one cycle with everyone
        # and one cycle without oncall users.
        start = datetime.fromtimestamp(start_time, utc)
        oncall_start_threshold = start - timedelta(days=14 * len(user_ids) - 7 * (len(ONCALL_USERS) + 1) - 4)

        cursor.execute(query, (team_id, user_ids, start_time, role_id))
        if cursor.rowcount != 0:
            rows = sorted(cursor.fetchall(), key=operator.itemgetter('last_end'))
            for r in rows:
                last_end = datetime.fromtimestamp(r['last_end'], utc)
                if r['user_id'] not in ONCALL_USERS or last_end <= oncall_start_threshold:
                    return r['user_id']
            return rows[0]['user_id']
        else:
            return None
