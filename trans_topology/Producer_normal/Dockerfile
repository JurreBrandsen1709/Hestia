# Use ASP.NET runtime image
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app

# Copy publish output from local machine
COPY ./star_topology/Producer_normal/out .
COPY ./star_topology/Producer_normal/normal.json /app/normal.json
COPY ./star_topology/Producer_normal/delay_w1.json /app/delay_w1.json
COPY ./star_topology/Producer_normal/w3_priority.json /app/w3_priority.json


ENTRYPOINT ["dotnet", "Producer.dll"]
