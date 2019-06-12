Invoke-Expression "cd C:\DistributedSystems\PA4"
for($i=1; $i -le 10; $i++){
Invoke-Expression ".\simpledynamo-grading.exe -p 5 -c -n 5 'C:\DistributedSystems\PA4\SimpleDynamo\app\build\outputs\apk\debug\app-debug.apk' > output$i.txt"
Start-Sleep -s 5
write-host "Iteration $i completed..."
}