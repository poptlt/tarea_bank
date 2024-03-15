# Может быть в задании предполагался cgi-скрипт,
# но я никогда не писал скриптов под Апач,
# поэтому решил, что проще будет не разбираясь а Апаче поднять свой микро-сервер
use LWP::Socket;
use URI;

use DBI;
use DBD::Pg;

my $config = do('./config.pl');

my $headers = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n";
 
my $sock = new LWP::Socket();
die "Не удалось подключить сокет" unless $sock->bind($config->{http_ip}, $config->{http_port});
$sock->listen(10);
 
while ( my $socket = $sock->accept(10) ) {

    my $data = '';

    $socket->read(\$_);

    my $url = /GET(.+)HTTP.+/s ? $1 : undef;
    $url = URI->new($url);

    my $address = $url->query_param('address');

    if ($address) {

        $data .= "<h3>Address: $address</h3>";

        my $dbh = DBI->connect(
            'dbi:Pg:dbname='.$config->{dbname}.'; host='.$config->{dbhost}.'; port='.$config->{dbport}, 
            $config->{username}, 
            $config->{password},
            {PrintError => 0}
        )
            or die "Не удалось подключиться к БД ($DBI::errstr)";

        # насколько понял смысл выборки в задании
        # если ошибся, то можно переписать после уточнения
        my $sth = $dbh->prepare("
            WITH
                -- по адресу выбираем строки из таблицы log с учетом конечного порядка и ограничения 
                from_log AS (
                    SELECT int_id, created, str
                        FROM log 
                        WHERE address = ?
                    ORDER BY int_id, created
                    LIMIT 101
                ),

                -- выбираем соответствующие им строки из таблицы message
                from_message AS (
                    SELECT message.int_id, message.created, message.str 
                        FROM from_log
                        JOIN message ON (from_log.int_id = message.int_id)
                    ORDER BY int_id, created
                    LIMIT 100
                )

            -- объеденяем упорядочивая и ограничивая количество
            SELECT * FROM from_log
            UNION ALL
            SELECT * FROM from_message
            ORDER BY int_id, created
            LIMIT 101
        ");

        $sth->execute($address) or die "Не удалось выполнить запрос к ДБ($DBI::errstr)";

        my $number = 0;     # номер строки выборки
        my $current = '';   # текущий идентификатор

        while (my ($int_id, $created, $str) = $sth->fetchrow) {

            $number++;

            if ($number <= 100) {

                if ($int_id ne $current) {

                    $data .= "</table>" if ($current);
                    $data .= "<h4>ID: $int_id</h4>";
                    $data .= "
                        <table>
                            <tr>
                                <th>number</th>
                                <th>created</th>
                                <th>str</th>
                            </tr>
                    ";
                    $current = $int_id;
                }

                $data .= "
                    <tr>
                        <td>$number</td>
                        <td>$created</td>
                        <td>$str</td>
                    </tr>
                ";
            }
        }

        $data .= "</table>" if $number > 0;
        $data .= "<h4>there's more strings</h4>" if $number > 100;

        $dbh->disconnect;
    }

    my $content = "
        <html>
            <body>
                <form>
                    <p>Address: <input type='text' name='address'></p>
                    <p><input type='submit' value='Send'></p>
                </form>
                $data
            </body>
        </html>
    ";

    $socket->write( $headers . $content );
    $socket->shutdown();
}
 
$sock->shutdown();