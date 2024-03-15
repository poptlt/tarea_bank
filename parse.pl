use DBI;
use DBD::Pg;

my $config = do('./config.pl');

open(my $fh, '<:encoding(UTF-8)', $config->{data_file})
    or die "Не удалось открыть файл с данными $!";

$dbh = DBI->connect(
    'dbi:Pg:dbname='.$config->{dbname}.'; host='.$config->{dbhost}.'; port='.$config->{dbport}, 
    $config->{username}, 
    $config->{password},
    {PrintError => 0}
)
    or die "Не удалось подключиться к БД ($DBI::errstr)";

# будем вставлять записи в таблицы по одной в транзакции (в задании на эту тему конкретики нет)
# если предполагалось, что необходима массовая загрузка, можно переписать
my $message_query = $dbh->prepare("INSERT INTO message(created, id, int_id, str) VALUES (?, ?, ?, ?)");
my $log_query = $dbh->prepare("INSERT INTO log(created, int_id, str, address) VALUES (?, ?, ?, ?)");

my @flags = ("<=", "=>", "->", "**", "==");

while (my $row = <$fh>) { 

    my @data = split ' ', $row;
    my ($date, $time) = (shift @data, shift @data);
    my $created = $date.' '.$time;

    my $flag = $data[1];
        
    if (grep { $flag eq $_ } @flags) {
    
        my $int_id = $data[0];
        my $str = join ' ', @data;

        if ($flag eq "<=") {

            # считаем, что если нет поля с id, то эта запись нам не нужна,
            # а если таких полей несколько, то используем первое попавшееся
            my @ids = grep { $_ } map { /^id=(.+)/; $1 } @data;
            my $id = $ids[0] ? $ids[0] : '';

            if ($id) {

                $message_query->execute($created, $id, $int_id, $str)
                    or die "Не удалось вставить запись в таблицу message ($DBI::errstr)";
            }
        }

        else {

            my $address = $data[2];

            $log_query->execute($created, $int_id, $str, $address)
                or die "Не удалось вставить запись в таблицу log ($DBI::errstr)";
        }
    }
}

$dbh->disconnect;