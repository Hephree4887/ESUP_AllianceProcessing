--THIS IS THE MSSQL VERSION OF WHAT WE NEED TO DO WITH MYSQL
--THIS IS FOR CLAUDE CODE TO USE FOR COMPAIRSON


set nocount on
set transaction isolation level read uncommitted
go
 
--Export data formatted to JSON files with 2500 pairs
declare @Batch bigint = 1,
        @BatchSize int = 2500,
        @LastRow bigint = 0,
        @Count int = 1,
        @FolderPath varchar(256) = concat('\\DBKPVCONVDB2\Alliance$\{Client}\Push', dbo.fnVariable('rptPush'), '\JSON\'),
        @FilePrefix varchar(256) = dbo.fnVariable('rptProjectName'),
        @CommunityId varchar(50) = 'Community ID from ACB',
        @Bcp varchar(8000),
        @Output varchar(max),
        @Result int
 
set @FolderPath = iif(right(@FolderPath, 1) <> '\', concat(@FolderPath, '\'), @FolderPath)
 
exec master.sys.xp_create_subdir @FolderPath
 
drop table if exists #Output
 
create table #Output (
    RowID int identity primary key,
    Data varchar(max)
)
 
select  @LastRow = max(EntityID)
from    PostScript.AllianceMerge
 
while 0=0 and @LastRow is not null
begin
    raiserror('%I64d', 0, 1, @Batch) with nowait
 
    select  @Bcp = concat('bcp "',
                'set nocount on ',
                'set transaction isolation level read uncommitted ',
                ' ',
                'select  try_convert(nvarchar(max), ( ',
                '        select  [CommunityId] = ''', @CommunityId, ''', ',
                '                [Entities] = ( ',
                '                    select  [Entity] = ( ',
                '                                select  [system] = m.ApplicationID, ',
                '                                        [type] = m.EntityType, ',
                '                                        [applicationId] = m.TargetID, ',
                '                                        [correlationid] = m.SourceIDValue ',
                '                                from    PostScript.AllianceMerge    m ',
                '                                where   m.EntityID = e.EntityID ',
                '                                order by m.[ApplicationID] ',
                '                                for json path) ',
                '                    from    PostScript.AllianceMerge    e ',
                '                    where   e.EntityID between ', @Batch, ' and ', @Batch + @BatchSize - 1, ' ',
                '                    group by e.EntityID ',
                '                    order by e.EntityID ',
                '                    for json path) ',
                '        for json path, without_array_wrapper)) ',
                '" queryout "', @FolderPath, @FilePrefix, @Count, '.json" -T -c -d', db_name(), ' -S', @@servername)
 
    insert into #Output (Data)
    exec @Result = master..xp_cmdshell @Bcp
 
    set @Output = null
 
    select  @Output = concat(@Output, char(10), Data)
    from    #Output
 
    if (@Result = 1)
    begin
        select  Error = @Output,
                Bcp = @Bcp
        break
    end
 
    if @Batch + @BatchSize > @LastRow
        break
 
    set @Batch = @Batch + @BatchSize
    set @Count = @Count + 1
end
go