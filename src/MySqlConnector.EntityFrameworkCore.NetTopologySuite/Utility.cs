using System;
using System.Linq;
using System.Reflection;

namespace MySqlConnector.EntityFrameworkCore.NetTopologySuite
{
	internal static class Utility
	{
		// copied from https://github.com/aspnet/EntityFrameworkCore/blob/master/src/Shared/MemberInfoExtensions.cs
		public static bool IsSameAs(this MemberInfo propertyInfo, MemberInfo otherPropertyInfo)
			=> propertyInfo == null
				? otherPropertyInfo == null
				: (otherPropertyInfo == null
					? false
					: Equals(propertyInfo, otherPropertyInfo)
					  || (propertyInfo.Name == otherPropertyInfo.Name
						  && (propertyInfo.DeclaringType == otherPropertyInfo.DeclaringType
							  || propertyInfo.DeclaringType.GetTypeInfo().IsSubclassOf(otherPropertyInfo.DeclaringType)
							  || otherPropertyInfo.DeclaringType.GetTypeInfo().IsSubclassOf(propertyInfo.DeclaringType)
							  || propertyInfo.DeclaringType.GetTypeInfo().ImplementedInterfaces.Contains(otherPropertyInfo.DeclaringType)
							  || otherPropertyInfo.DeclaringType.GetTypeInfo().ImplementedInterfaces.Contains(propertyInfo.DeclaringType))));

		public static MemberInfo OnInterface(this MemberInfo targetMember, Type interfaceType)
		{
			var declaringType = targetMember.DeclaringType;
			if (declaringType == interfaceType
				|| declaringType.IsInterface
				|| !declaringType.GetInterfaces().Any(i => i == interfaceType))
			{
				return targetMember;
			}
			if (targetMember is MethodInfo targetMethod)
			{
				return targetMethod.OnInterface(interfaceType);
			}
			if (targetMember is PropertyInfo targetProperty)
			{
				var targetGetMethod = targetProperty.GetMethod;
				var interfaceGetMethod = targetGetMethod.OnInterface(interfaceType);
				if (interfaceGetMethod == targetGetMethod)
				{
					return targetProperty;
				}

				return interfaceType.GetProperties().First(p => Equals(p.GetMethod, interfaceGetMethod));
			}

			return targetMember;
		}

		public static MethodInfo OnInterface(this MethodInfo targetMethod, Type interfaceType)
		{
			var declaringType = targetMethod.DeclaringType;
			if (declaringType == interfaceType
				|| declaringType.IsInterface
				|| !declaringType.GetInterfaces().Any(i => i == interfaceType))
			{
				return targetMethod;
			}

			var map = targetMethod.DeclaringType.GetInterfaceMap(interfaceType);
			for (var i = 0; i < map.TargetMethods.Length; i++)
			{
				if (map.TargetMethods[i].IsSameAs(targetMethod))
					return map.InterfaceMethods[i];
			}
			return targetMethod;
		}
	}
}
